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
package ideal.sylph.runner.spark.structured;

import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.etl.OperatorType;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.etl.api.Sink;
import ideal.sylph.etl.api.Source;
import ideal.sylph.etl.api.TransForm;
import ideal.sylph.runner.spark.SparkRecord;
import ideal.sylph.runner.spark.sparkstreaming.StreamNodeLoader;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.OperatorMetaData;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.github.harbby.gadtry.base.Throwables.throwsThrowable;

/**
 * Created by ideal on 17-5-8.
 */
public class StructuredNodeLoader
        implements NodeLoader<Dataset<Row>>
{
    private final Logger logger = LoggerFactory.getLogger(StructuredNodeLoader.class);

    private final OperatorMetaData operatorMetaData;
    private final IocFactory iocFactory;

    public StructuredNodeLoader(OperatorMetaData operatorMetaData, IocFactory iocFactory)
    {
        this.operatorMetaData = operatorMetaData;
        this.iocFactory = iocFactory;
    }

    @Override
    public UnaryOperator<Dataset<Row>> loadSource(String driverStr, Map<String, Object> config)
    {
        Class<?> driverClass = operatorMetaData.getConnectorDriver(driverStr, OperatorType.source);
        Source<Dataset<Row>> source = (Source<Dataset<Row>>) getPluginInstance(driverClass, config);

        return stream -> {
            source.getSource().printSchema();
            return source.getSource();
        };
    }

    @Override
    public UnaryOperator<Dataset<Row>> loadSink(String driverStr, Map<String, Object> config)
    {
        return stream -> {
            //-------启动job-------
            StreamingQuery streamingQuery = loadSinkWithComplic(driverStr, config).apply(stream).start(""); //start job
            //streamingQuery.stop()
            return null;
        };
    }

    public Function<Dataset<Row>, DataStreamWriter<Row>> loadSinkWithComplic(String driverStr, Map<String, Object> config)
    {
        Class<?> driverClass = operatorMetaData.getConnectorDriver(driverStr, OperatorType.sink);
        Object driver = getPluginInstance(driverClass, config);

        final Sink<DataStreamWriter<Row>> sink;
        if (driver instanceof RealTimeSink) {
            sink = loadRealTimeSink((RealTimeSink) driver);
        }
        else if (driver instanceof Sink) {
            sink = (Sink<DataStreamWriter<Row>>) driver;
        }
        else {
            throw new RuntimeException("未知的sink插件:" + driver);
        }

        logger.info("初始化{} 完成", driver);

        return stream -> {
            //-------启动job-------
            DataStreamWriter<Row> writer = stream.writeStream();
            if (config.containsKey("outputMode")) { //设置输出模式
                writer.outputMode((String) config.get("outputMode"));
            }
            String jobName = (String) config.get("name");
            writer.queryName(jobName);
            //writer.trigger(Trigger.ProcessingTime("1 seconds")); //设置微批处理触发器

            //--checkpoint 周期为90秒
            //see: http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#experimental
            writer.trigger(Trigger.Continuous("90 second"));        // only change in query 真正流计算连续处理,没有容错保证。失败则只能重启从上个检查点开始恢复
            if (config.containsKey("checkpoint")) {
                writer.option("checkpointLocation", (String) config.get("checkpoint"));
            }
            sink.run(writer);
            return writer;
        };
    }

    /**
     * transform api 尝试中
     **/
    @Override
    public UnaryOperator<Dataset<Row>> loadTransform(String driverStr, Map<String, Object> config)
    {
        Class<?> driverClass = operatorMetaData.getConnectorDriver(driverStr, OperatorType.transform);
        Object driver = getPluginInstance(driverClass, config);

        final TransForm<Dataset<Row>> transform;
        if (driver instanceof RealTimeTransForm) {
            transform = loadRealTimeTransForm((RealTimeTransForm) driver);
        }
        else if (driver instanceof TransForm) {
            transform = (TransForm<Dataset<Row>>) driver;
        }
        else {
            throw new RuntimeException("unknown Transform plugin:" + driver);
        }

        return stream -> {
            Dataset<Row> transStream = transform.transform(stream);
            logger.info("{} schema to :", driver);
            transStream.printSchema();
            return transStream;
        };
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
                    throw throwsThrowable(e);
                }
            }
        });
    }

    private static TransForm<Dataset<Row>> loadRealTimeTransForm(RealTimeTransForm realTimeTransForm)
    {
        return stream -> {
            //spark2.x 要对dataSet 进行map操作必须要加上下面一句类型映射 即必须要指明返回的schema
            //implicit val matchError:org.apache.spark.sql.Encoder[Row] = org.apache.spark.sql.Encoders.kryo[Row]
            //      import collection.JavaConverters._
            //      val mapRowSchema = realTimeTransForm.getRowSchema.getFields.asScala.map(filed => {
            //        StructField(filed.getName, SparkRow.SparkRowParser.parserType(filed.getJavaType), true)
            //      })
            //      RowEncoder.apply(StructType(mapRowSchema))

            //implicit val mapenc = RowEncoder.apply(rddSchema)  //此处无法注册 原因是必须是sql基本类型   //Encoders.STRING
            Dataset<Row> transStream = stream.mapPartitions(
                    (MapPartitionsFunction<Row, Row>) partition -> StreamNodeLoader.transFunction(partition, realTimeTransForm),
                    Encoders.kryo(Row.class));
            //或者使用 transStream.as()
            return transStream;
        };
    }

    @Override
    public IocFactory getIocFactory()
    {
        return iocFactory;
    }
}
