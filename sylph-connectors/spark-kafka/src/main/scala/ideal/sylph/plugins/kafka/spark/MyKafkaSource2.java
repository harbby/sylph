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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ideal.sylph.runner.spark.SQLHepler.schemaToSparkType;
import static java.nio.charset.StandardCharsets.UTF_8;

@Name("kafka")
@Version("1.0.0")
@Description("this spark kafka 0.10+ source inputStream")
public class MyKafkaSource2
        implements Source<DStream<Row>>
{
    private final transient Supplier<DStream<Row>> loadStream;

    public MyKafkaSource2(JavaStreamingContext ssc, KafkaSourceConfig config, SourceContext context)
    {
        this.loadStream = Lazys.goLazy(() -> createSource(ssc, config, context));
    }

    public DStream<Row> createSource(JavaStreamingContext ssc, KafkaSourceConfig config, SourceContext context)
    {
        String topics = config.getTopics();
        String brokers = config.getBrokers(); //需要把集群的host 配置到程序所在机器
        String groupId = config.getGroupid(); //消费者的名字
        String offsetMode = config.getOffsetMode();

        Map<String, Object> kafkaParams = new HashMap<>(config.getOtherConfig());
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class); //StringDeserializer
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class); //StringDeserializer
        kafkaParams.put("enable.auto.commit", false); //不自动提交偏移量
        //      "fetch.message.max.bytes" ->
        //      "session.timeout.ms" -> "30000", //session默认是30秒
        //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期
        kafkaParams.put("group.id", groupId); //注意不同的流 group.id必须要不同 否则会出现offect commit提交失败的错误
        kafkaParams.put("auto.offset.reset", offsetMode); //latest   earliest

        Set<String> topicSets = Arrays.stream(topics.split(",")).collect(Collectors.toSet());
        JavaInputDStream<ConsumerRecord<byte[], byte[]>> inputStream = KafkaUtils.createDirectStream(
                ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicSets, kafkaParams));

        if ("json".equalsIgnoreCase(config.getValueType())) {
            JsonSchema jsonParser = new JsonSchema(context.getSchema());
            return inputStream
                    .map(record -> jsonParser.deserialize(record.key(), record.value(), record.topic(), record.partition(), record.offset()))
                    .dstream();
        }
        else {
            StructType structType = schemaToSparkType(context.getSchema());
            return inputStream
                    .map(record -> {
                        String[] names = structType.names();
                        Object[] values = new Object[names.length];
                        for (int i = 0; i < names.length; i++) {
                            switch (names[i]) {
                                case "_topic":
                                    values[i] = record.topic();
                                    continue;
                                case "_message":
                                    values[i] = new String(record.value(), UTF_8);
                                    continue;
                                case "_key":
                                    values[i] = new String(record.key(), UTF_8);
                                    continue;
                                case "_partition":
                                    values[i] = record.partition();
                                    continue;
                                case "_offset":
                                    values[i] = record.offset();
                                case "_timestamp":
                                    values[i] = record.timestamp();
                                case "_timestampType":
                                    values[i] = record.timestampType().id;
                                default:
                                    values[i] = null;
                            }
                        }
                        return (Row) new GenericRowWithSchema(values, structType);
                    })
                    .dstream();  //.window(Duration(10 * 1000))
        }
    }

    @Override
    public DStream<Row> getSource()
    {
        return loadStream.get();
    }
}
