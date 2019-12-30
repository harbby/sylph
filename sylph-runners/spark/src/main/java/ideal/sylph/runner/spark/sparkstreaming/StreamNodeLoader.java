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
package ideal.sylph.runner.spark.sparkstreaming;

import com.github.harbby.gadtry.base.JavaTypes;
import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.etl.Operator;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.etl.api.Sink;
import ideal.sylph.etl.api.Source;
import ideal.sylph.etl.api.TransForm;
import ideal.sylph.runner.spark.SparkRecord;
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.NodeLoader;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * Created by ideal on 17-5-8.
 * spark 1.x spark Streaming
 */
public class StreamNodeLoader
        implements NodeLoader<JavaDStream<Row>>
{
    private static final Type typeDStream = JavaTypes.make(DStream.class, new Type[] {Row.class}, null);
    private static final Type typeJavaDStream = JavaTypes.make(JavaDStream.class, new Type[] {Row.class}, null);
    private static final Type typeJavaRDD = JavaTypes.make(JavaRDD.class, new Type[] {Row.class}, null);
    private static final Type typeRDD = JavaTypes.make(RDD.class, new Type[] {Row.class}, null);

    private final ConnectorStore connectorStore;
    private final IocFactory iocFactory;

    public StreamNodeLoader(ConnectorStore connectorStore, IocFactory iocFactory)
    {
        this.connectorStore = connectorStore;
        this.iocFactory = iocFactory;
    }

    @Override
    public UnaryOperator<JavaDStream<Row>> loadSource(String driverStr, Map<String, Object> config)
    {
        Class<?> driverClass = connectorStore.getConnectorDriver(driverStr, Operator.PipelineType.source);
        checkState(Source.class.isAssignableFrom(driverClass));

        checkState(driverClass.getGenericInterfaces()[0] instanceof ParameterizedType);
        ParameterizedType sourceType = (ParameterizedType) driverClass.getGenericInterfaces()[0];
        checkState(sourceType.getRawType() == Source.class);

        Source<?> source = (Source<?>) getPluginInstance(driverClass, config);
        if (sourceType.getActualTypeArguments()[0].equals(typeDStream)) {
            return stream -> {
                DStream<Row> input = (DStream<Row>) source.getSource();
                return JavaDStream.fromDStream(input, JavaSparkContext$.MODULE$.fakeClassTag());
            };
        }
        else if (sourceType.getActualTypeArguments()[0].equals(typeJavaDStream)) {
            return stream -> (JavaDStream<Row>) source.getSource();
        }
        else {
            throw new UnsupportedOperationException("Unsupported sparkStreaming Source " + driverClass + " type:" + sourceType);
        }
    }

    public Consumer<JavaRDD<Row>> loadRDDSink(String driverStr, Map<String, Object> config)
    {
        Class<?> driverClass = connectorStore.getConnectorDriver(driverStr, Operator.PipelineType.sink);
        Object driver = getPluginInstance(driverClass, config);

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
        return sink::run;
    }

    @Override
    public UnaryOperator<JavaDStream<Row>> loadSink(String driverStr, Map<String, Object> config)
    {
        Consumer<JavaRDD<Row>> sink = this.loadRDDSink(driverStr, config);
        return stream -> {
            //DStreamUtil.dstreamParser(stream, sink) //这里处理偏移量提交问题
            stream.foreachRDD(sink::accept);
            return null;
        };
    }

    /**
     * transform api 尝试中
     **/
    @Override
    public UnaryOperator<JavaDStream<Row>> loadTransform(String driverStr, Map<String, Object> config)
    {
        Class<?> driverClass = connectorStore.getConnectorDriver(driverStr, Operator.PipelineType.transform);
        Object driver = getPluginInstance(driverClass, config);

        final TransForm<JavaDStream<Row>> transform;
        if (driver instanceof RealTimeTransForm) {
            transform = loadRealTimeTransForm((RealTimeTransForm) driver);
        }
        else if (driver instanceof TransForm) {
            checkState(driverClass.getGenericInterfaces()[0] instanceof ParameterizedType);
            ParameterizedType transformType = (ParameterizedType) driverClass.getGenericInterfaces()[0];
            if (transformType.getActualTypeArguments()[0].equals(typeJavaDStream)) {
                transform = (TransForm<JavaDStream<Row>>) driver;
            }
            else if (transformType.getActualTypeArguments()[0].equals(typeDStream)) {
                transform = rowJavaRDD -> JavaDStream.fromDStream(((TransForm<DStream<Row>>) driver).transform(rowJavaRDD.dstream()),
                        JavaSparkContext$.MODULE$.fakeClassTag());
            }
            else {
                throw new UnsupportedOperationException("Unsupported sparkStreaming Transform" + driverClass + " type:" + transformType);
            }
        }
        else {
            throw new RuntimeException("unknown Transform plugin:" + driver);
        }

        return transform::transform;
    }

    private static Sink<JavaRDD<Row>> loadRealTimeSink(RealTimeSink realTimeSink)
    {
        return (Sink<JavaRDD<Row>>) rdd -> rdd.foreachPartition(partition -> {
            Throwable errorOrNull = null;
            try {
                int partitionId = TaskContext.getPartitionId();
                boolean openOK = realTimeSink.open(partitionId, 0); //初始化 返回是否正常 如果正常才处理数据
                if (openOK) {
                    partition.forEachRemaining(row -> realTimeSink.process(SparkRecord.make(row)));
                }
            }
            catch (Exception e) {
                errorOrNull = e; //open出错了
            }
            finally {
                realTimeSink.close(errorOrNull); //destroy()
            }
        });
    }

    public static Iterator<Row> transFunction(Iterator<Row> partition, RealTimeTransForm realTimeTransForm)
    {
        Exception errorOrNull = null;
        Schema schema = realTimeTransForm.getSchema(); // if not null
        List<Row> list = new ArrayList<>();
        try {
            int partitionId = TaskContext.getPartitionId();
            if (realTimeTransForm.open(partitionId, 0)) {
                partition.forEachRemaining(row -> {
                    realTimeTransForm.process(SparkRecord.make(row), (transOutrow) -> {
                        //TODO: SparkRow.parserRow(x) with schema ?
                        list.add(SparkRecord.parserRow(transOutrow));
                    });
                });
            }
        }
        catch (Exception e) {
            errorOrNull = e; //转换失败 这批数据都丢弃
        }
        finally {
            realTimeTransForm.close(errorOrNull); //destroy()
        }
        return list.iterator();
    }

    private static TransForm<JavaDStream<Row>> loadRealTimeTransForm(RealTimeTransForm realTimeTransForm)
    {
        return stream -> stream.mapPartitions(partition -> transFunction(partition, realTimeTransForm));
    }

    @Override
    public IocFactory getIocFactory()
    {
        return iocFactory;
    }
}
