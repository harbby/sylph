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
package ideal.sylph.plugins.kafka.spark;

import com.github.harbby.gadtry.base.Lazys;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.etl.api.Source;
import ideal.sylph.plugins.kafka.spark.structured.KafkaSourceUtil;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static ideal.sylph.runner.spark.SQLHepler.schemaToSparkType;

@Name("kafka")
@Version("1.0.0")
@Description("this spark kafka 0.10+ source inputStream")
public class StructuredKafkaSource
        implements Source<Dataset<Row>>
{
    private final transient Supplier<Dataset<Row>> loadStream;

    public StructuredKafkaSource(SparkSession spark, KafkaSourceConfig config, SourceContext context)
    {
        this.loadStream = Lazys.goLazy(() -> createSource(spark, config, context));
    }

    private static Dataset<Row> createSource(SparkSession spark, KafkaSourceConfig config, SourceContext context)
    {
        String topics = config.getTopics();
        String brokers = config.getBrokers(); //需要把集群的host 配置到程序所在机器
        String groupId = config.getGroupid(); //消费者的名字
        String offsetMode = config.getOffsetMode();

        checkState(!"largest".equals(offsetMode), "kafka 0.10+, use latest");
        checkState(!"smallest".equals(offsetMode), "kafka 0.10+, use earliest");

        Map<String, Object> kafkaParams = new HashMap<>(config.getOtherConfig());
        kafkaParams.put("subscribe", topics);
        kafkaParams.put("kafka.bootstrap.servers", brokers);
        kafkaParams.put("startingOffsets", offsetMode); //latest   earliest

        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class.getName()); //StringDeserializer
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class.getName()); //StringDeserializer
        //      "fetch.message.max.bytes" ->
        //      "session.timeout.ms" -> "30000", //session默认是30秒
        //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期

        Dataset<Row> inputStream = KafkaSourceUtil.getSource(spark, kafkaParams);
        if ("json".equalsIgnoreCase(config.getValueType())) {
            JsonSchema jsonParser = new JsonSchema(context.getSchema());
            return inputStream
                    .map((MapFunction<Row, Row>) record -> {
                        return jsonParser.deserialize(record.getAs("key"),
                                record.getAs("value"),
                                record.<String>getAs("topic"),
                                record.<Integer>getAs("partition"),
                                record.<Long>getAs("offset"));
                    }, RowEncoder.apply(jsonParser.getProducedType()));
        }
        else {
            StructType structType = schemaToSparkType(context.getSchema());
            return inputStream
                    .map((MapFunction<Row, Row>) record -> {
                        String[] names = structType.names();
                        Object[] values = new Object[names.length];
                        for (int i = 0; i < names.length; i++) {
                            switch (names[i]) {
                                case "_topic":
                                    values[i] = record.<String>getAs("topic");
                                    continue;
                                case "_message":
                                    values[i] = record.getAs("value");
                                    continue;
                                case "_key":
                                    values[i] = record.getAs("key");
                                    continue;
                                case "_partition":
                                    values[i] = record.<Integer>getAs("partition");
                                    continue;
                                case "_offset":
                                    values[i] = record.<Long>getAs("offset");
                                default:
                                    values[i] = null;
                            }
                        }
                        return (Row) new GenericRowWithSchema(values, structType);
                    }, RowEncoder.apply(structType));
        }
    }

    @Override
    public Dataset<Row> getSource()
    {
        return loadStream.get();
    }
}
