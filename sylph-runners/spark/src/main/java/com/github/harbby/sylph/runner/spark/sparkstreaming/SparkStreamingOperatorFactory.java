/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.sylph.runner.spark.sparkstreaming;

import com.github.harbby.gadtry.base.JavaTypes;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.sylph.api.RealTimeSink;
import com.github.harbby.sylph.api.Sink;
import com.github.harbby.sylph.api.Source;
import com.github.harbby.sylph.runner.spark.SparkRecord;
import com.github.harbby.sylph.spi.OperatorFactory;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.sylph.spi.utils.PluginFactory.getPluginInstance;

/**
 * Created by ideal on 17-5-8.
 * spark 1.x spark Streaming
 */
public class SparkStreamingOperatorFactory
        implements OperatorFactory<JavaDStream<Row>>
{
    private static final Type typeDStream = JavaTypes.make(DStream.class, new Type[] {Row.class}, null);
    private static final Type typeJavaDStream = JavaTypes.make(JavaDStream.class, new Type[] {Row.class}, null);
    private static final Type typeJavaRDD = JavaTypes.make(JavaRDD.class, new Type[] {Row.class}, null);
    private static final Type typeRDD = JavaTypes.make(RDD.class, new Type[] {Row.class}, null);

    private final IocFactory iocFactory;

    public SparkStreamingOperatorFactory(IocFactory iocFactory)
    {
        this.iocFactory = iocFactory;
    }

    @Override
    public Source<JavaDStream<Row>> createSource(Class<?> driverClass, Map<String, Object> config)
    {
        checkState(Source.class.isAssignableFrom(driverClass));

        checkState(driverClass.getGenericInterfaces()[0] instanceof ParameterizedType);
        ParameterizedType sourceType = (ParameterizedType) driverClass.getGenericInterfaces()[0];
        checkState(sourceType.getRawType() == Source.class);

        Source<?> source = (Source<?>) getPluginInstance(iocFactory, driverClass, config);
        if (sourceType.getActualTypeArguments()[0].equals(typeDStream)) {
            return () -> {
                DStream<Row> input = (DStream<Row>) source.createSource();
                return JavaDStream.fromDStream(input, JavaSparkContext$.MODULE$.fakeClassTag());
            };
        }
        else if (sourceType.getActualTypeArguments()[0].equals(typeJavaDStream)) {
            return (Source<JavaDStream<Row>>) source;
        }
        else {
            throw new UnsupportedOperationException("Unsupported sparkStreaming Source " + driverClass + " type:" + sourceType);
        }
    }

    public Sink<JavaRDD<Row>> loadRDDSink(Class<?> driverClass, Map<String, Object> config)
    {
        Object driver = getPluginInstance(iocFactory, driverClass, config);

        final Sink<JavaRDD<Row>> sink;
        if (driver instanceof RealTimeSink) {
            sink = loadRealTimeSink((RealTimeSink) driver);
        }
        else if (driver instanceof Sink) {
            checkState(driverClass.getGenericInterfaces()[0] instanceof ParameterizedType);
            ParameterizedType sinkType = (ParameterizedType) driverClass.getGenericInterfaces()[0];
            if (sinkType.getActualTypeArguments()[0].equals(typeJavaRDD)) {
                sink = (Sink<JavaRDD<Row>>) driver;
            }
            else if (sinkType.getActualTypeArguments()[0].equals(typeRDD)) {
                sink = rowJavaRDD -> ((Sink<RDD<Row>>) driver).run(rowJavaRDD.rdd());
            }
            else {
                throw new UnsupportedOperationException("Unsupported sparkStreaming Sink" + driverClass + " type:" + sinkType);
            }
        }
        else {
            throw new RuntimeException("unknown sink type:" + driver);
        }
        return sink;
    }

    @Override
    public Sink<JavaDStream<Row>> createSink(Class<?> driverClass, Map<String, Object> config)
            throws ClassNotFoundException
    {
        Sink<JavaRDD<Row>> sink = this.loadRDDSink(driverClass, config);
        return stream -> {
            //DStreamUtil.dstreamParser(stream, sink) //commit offset
            stream.foreachRDD(sink::run);
        };
    }

    private static Sink<JavaRDD<Row>> loadRealTimeSink(RealTimeSink realTimeSink)
    {
        return rdd -> rdd.foreachPartition(partition -> {
            Throwable errorOrNull = null;
            try {
                int partitionId = TaskContext.getPartitionId();
                boolean openOK = realTimeSink.open(partitionId, 0);
                if (openOK) {
                    partition.forEachRemaining(row -> realTimeSink.process(SparkRecord.make(row)));
                }
            }
            catch (Exception e) {
                errorOrNull = e;
            }
            finally {
                realTimeSink.close(errorOrNull);
            }
        });
    }
}
