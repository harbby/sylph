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
package com.github.harbby.sylph.runner.spark.structured;

import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.sylph.api.RealTimeSink;
import com.github.harbby.sylph.api.Sink;
import com.github.harbby.sylph.api.Source;
import com.github.harbby.sylph.runner.spark.SparkRecord;
import com.github.harbby.sylph.spi.OperatorFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.github.harbby.gadtry.base.Throwables.throwThrowable;
import static com.github.harbby.sylph.spi.utils.PluginFactory.getPluginInstance;

/**
 * Created by ideal on 17-5-8.
 */
public class StructuredNodeLoader
        implements OperatorFactory<Dataset<Row>>
{
    private final Logger logger = LoggerFactory.getLogger(StructuredNodeLoader.class);

    private final IocFactory iocFactory;
    private final boolean isCompile;
    private final String checkpointLocation = "hdfs:///tmp/sylph/spark/savepoints/";

    public StructuredNodeLoader(IocFactory iocFactory, boolean isCompile)
    {
        this.iocFactory = iocFactory;
        this.isCompile = isCompile;
    }

    @Override
    public Source<Dataset<Row>> createSource(Class<?> driverClass, Map<String, Object> config)
    {
        Source<Dataset<Row>> source = (Source<Dataset<Row>>) getPluginInstance(iocFactory, driverClass, config);
        return () -> {
            Dataset<Row> dataset = source.createSource();
            dataset.printSchema();
            return dataset;
        };
    }

    @Override
    public Sink<Dataset<Row>> createSink(Class<?> driverClass, Map<String, Object> config)
    {
        Sink<DataStreamWriter<Row>> userSink = createUserSink(driverClass, config);
        return stream -> {
            //-------启动job-------
            //-------启动job-------
            DataStreamWriter<Row> writer = stream.writeStream();
            if (config.containsKey("outputMode")) { //设置输出模式
                writer.outputMode((String) config.get("outputMode"));
            }
            String jobName = (String) config.get("name");
            writer.queryName(jobName);
            //--checkpoint
            //see: http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#experimental
            writer.trigger(Trigger.Continuous("90 second"));
            if (config.containsKey("checkpoint")) {
                writer.option("checkpointLocation", (String) config.get("checkpoint"));
            }
            userSink.run(writer);
            if (!isCompile) {
                //UnsupportedOperationChecker.checkForContinuous();
                writer.option("checkpointLocation", checkpointLocation);
                writer.start("");
            }
        };
    }

    private Sink<DataStreamWriter<Row>> createUserSink(Class<?> driverClass, Map<String, Object> config)
    {
        Object driver = getPluginInstance(iocFactory, driverClass, config);

        final Sink<DataStreamWriter<Row>> sink;
        if (driver instanceof RealTimeSink) {
            sink = loadRealTimeSink((RealTimeSink) driver);
        }
        else if (driver instanceof Sink) {
            sink = (Sink<DataStreamWriter<Row>>) driver;
        }
        else {
            throw new RuntimeException("unknown sink connector:" + driver);
        }
        return sink;
    }

    private static Sink<DataStreamWriter<Row>> loadRealTimeSink(RealTimeSink realTimeSink)
    {
        return stream -> stream.foreach(new ForeachWriter<Row>()
        {
            @Override
            public void process(Row value)
            {
                realTimeSink.process(SparkRecord.make(value));
            }

            @Override
            public void close(Throwable errorOrNull)
            {
                realTimeSink.close(errorOrNull);
            }

            @Override
            public boolean open(long partitionId, long version)
            {
                try {
                    return realTimeSink.open(partitionId, version);
                }
                catch (Exception e) {
                    throw throwThrowable(e);
                }
            }
        });
    }
}
