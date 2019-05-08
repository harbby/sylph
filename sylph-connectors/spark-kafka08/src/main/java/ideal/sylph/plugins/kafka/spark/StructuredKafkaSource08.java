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

import com.github.harbby.spark.sql.kafka.KafkaDataSource08;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.etl.api.Source;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ideal.sylph.runner.spark.SQLHepler.schemaToSparkType;
import static java.util.Objects.requireNonNull;

@Name("kafka08")
@Version("1.0.0")
@Description("this Spark Structured kafka 0.8 source inputStream")
public class StructuredKafkaSource08
        implements Source<Dataset<Row>>
{
    private final transient Supplier<Dataset<Row>> loadStream;

    public StructuredKafkaSource08(SparkSession spark, KafkaSourceConfig08 config, SourceContext context)
    {
        this.loadStream = () -> createSource(spark, config, context);
    }

    public Dataset<Row> createSource(SparkSession spark, KafkaSourceConfig08 config, SourceContext context)
    {
        String topics = requireNonNull(config.getTopics(), "topics not setting");
        String brokers = requireNonNull(config.getBrokers(), "brokers not setting"); //需要把集群的host 配置到程序所在机器
        String groupId = requireNonNull(config.getGroupid(), "group.id not setting"); //消费者的名字
        String offsetMode = requireNonNull(config.getOffsetMode(), "offsetMode not setting");

        Map<String, String> otherConfig = config.getOtherConfig().entrySet()
                .stream()
                .filter(x -> x.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().toString()));

        Map<String, String> kafkaParams = new HashMap<>(otherConfig);
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        //kafkaParams.put("auto.commit.enable", true); //不自动提交偏移量
        //      "fetch.message.max.bytes" ->
        //      "session.timeout.ms" -> "30000", //session默认是30秒
        //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); //注意不同的流 group.id必须要不同 否则会出现offect commit提交失败的错误
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetMode); //largest   smallest

        Dataset<Row> kafka08 = spark.readStream()
                .format(KafkaDataSource08.class.getName())
                .option("topics", topics)
                .options(kafkaParams)
                .load();

        if ("json".equalsIgnoreCase(config.getValueType())) {
            JsonSchema jsonParser = new JsonSchema(context.getSchema());
            return kafka08
                    .map((MapFunction<Row, Row>) record -> {
                        return jsonParser.deserialize(
                                record.getAs("_key"),
                                record.getAs("_message"),
                                record.<String>getAs("_topic"),
                                record.<Integer>getAs("_partition"),
                                record.<Long>getAs("_offset"));
                    }, RowEncoder.apply(jsonParser.getProducedType()));
        }
        else {
            StructType structType = schemaToSparkType(context.getSchema());
            String[] columns = Arrays.stream(structType.names()).map(name -> {
                switch (name) {
                    case "_key":
                        return "CAST(_key AS STRING) as _key";
                    case "_message":
                        return "CAST(_message AS STRING) as _message";
                    default:
                        return name;
                }
            }).toArray(String[]::new);
            return kafka08.selectExpr(columns); //对输入的数据进行 cast转换
        }
    }

    @Override
    public Dataset<Row> getSource()
    {
        return loadStream.get();
    }
}
