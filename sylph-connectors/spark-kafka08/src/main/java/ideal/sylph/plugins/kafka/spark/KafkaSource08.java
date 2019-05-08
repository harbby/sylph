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
import com.github.harbby.spark.sql.kafka.KafkaOffsetCommitter;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.etl.api.Source;
import ideal.sylph.runner.spark.kafka.SylphKafkaOffset;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.KafkaUtils$;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag$;
import scala.util.Either;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ideal.sylph.runner.spark.SQLHepler.schemaToSparkType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Name("kafka08")
@Version("1.0.0")
@Description("this spark kafka 0.8 source inputStream")
public class KafkaSource08
        implements Source<JavaDStream<Row>>
{
    private final transient Supplier<JavaDStream<Row>> loadStream;

    public KafkaSource08(JavaStreamingContext ssc, KafkaSourceConfig08 config, SourceContext context)
    {
        this.loadStream = Lazys.goLazy(() -> createSource(ssc, config, context));
    }

    public JavaDStream<Row> createSource(JavaStreamingContext ssc, KafkaSourceConfig08 config, SourceContext context)
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

        //----get fromOffsets
        @SuppressWarnings("unchecked")
        scala.collection.immutable.Map<String, String> map = (scala.collection.immutable.Map<String, String>) Map$.MODULE$.apply(JavaConverters.mapAsScalaMapConverter(kafkaParams).asScala().toSeq());
        final KafkaCluster kafkaCluster = new KafkaCluster(map);
        Map<TopicAndPartition, Long> fromOffsets = getFromOffset(kafkaCluster, topics, groupId);

        //--- createDirectStream  DirectKafkaInputDStream.class
        org.apache.spark.api.java.function.Function<MessageAndMetadata<byte[], byte[]>, ConsumerRecord<byte[], byte[]>> messageHandler =
                mmd -> new ConsumerRecord<>(mmd.topic(), mmd.partition(), mmd.key(), mmd.message(), mmd.offset());
        @SuppressWarnings("unchecked")
        Class<ConsumerRecord<byte[], byte[]>> recordClass = (Class<ConsumerRecord<byte[], byte[]>>) ClassTag$.MODULE$.<ConsumerRecord<byte[], byte[]>>apply(ConsumerRecord.class).runtimeClass();
        JavaInputDStream<ConsumerRecord<byte[], byte[]>> inputStream = KafkaUtils.createDirectStream(ssc,
                byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class, recordClass,
                kafkaParams, fromOffsets,
                messageHandler
        );
        JavaDStream<ConsumerRecord<byte[], byte[]>> dStream = settingCommit(inputStream, kafkaParams, kafkaCluster, groupId);

        if ("json".equalsIgnoreCase(config.getValueType())) {
            JsonSchema jsonParser = new JsonSchema(context.getSchema());
            return dStream
                    .map(record -> {
                        return jsonParser.deserialize(record.key(), record.value(), record.topic(), record.partition(), record.offset());
                    });
        }
        else {
            StructType structType = schemaToSparkType(context.getSchema());
            return dStream
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
                                default:
                                    values[i] = null;
                            }
                        }
                        return (Row) new GenericRowWithSchema(values, structType);
                    });  //.window(Duration(10 * 1000))
        }
    }

    public static Map<TopicAndPartition, Long> getFromOffset(KafkaCluster kafkaCluster, String topics, String groupId)
    {
        Set<String> topicSets = Arrays.stream(topics.split(",")).collect(Collectors.toSet());
        return getFromOffset(kafkaCluster, topicSets, groupId);
    }

    public static Map<TopicAndPartition, Long> getFromOffset(KafkaCluster kafkaCluster, Set<String> topics, String groupId)
    {
        scala.collection.immutable.Set<String> scalaTopicSets = JavaConverters.asScalaSetConverter(topics).asScala().toSet();

        Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, Object>> groupOffsets = kafkaCluster.getConsumerOffsets(groupId,
                kafkaCluster.getPartitions(scalaTopicSets).right().get());

        scala.collection.immutable.Map<TopicAndPartition, Object> fromOffsets;
        if (groupOffsets.isRight()) {
            fromOffsets = groupOffsets.right().get();
        }
        else {
            fromOffsets = KafkaUtils$.MODULE$.getFromOffsets(kafkaCluster, kafkaCluster.kafkaParams(), scalaTopicSets);
        }
        return JavaConverters.mapAsJavaMapConverter(fromOffsets).asJava().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> (long) v.getValue()));
    }

    private static JavaDStream<ConsumerRecord<byte[], byte[]>> settingCommit(
            JavaInputDStream<ConsumerRecord<byte[], byte[]>> inputStream,
            Map<String, String> kafkaParams,
            KafkaCluster kafkaCluster,
            String groupId)
    {
        if (kafkaParams.getOrDefault("auto.commit.enable", "true").equals("false")) {
            return inputStream;
        }

        int commitInterval = Integer.parseInt(kafkaParams.getOrDefault(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "90000"));

        DStream<ConsumerRecord<byte[], byte[]>> sylphKafkaOffset = new SylphKafkaOffset<ConsumerRecord<byte[], byte[]>>(inputStream.inputDStream())
        {
            private final KafkaOffsetCommitter thread = new KafkaOffsetCommitter(
                    kafkaCluster,
                    groupId,
                    commitInterval);

            @Override
            public void initialize(Time time)
            {
                super.initialize(time);
                thread.setName("Kafka_Offset_Committer");
                thread.start();
            }

            @Override
            public void commitOffsets(RDD<?> kafkaRdd)
            {
                OffsetRange[] offsets = ((HasOffsetRanges) kafkaRdd).offsetRanges();
                Map<TopicAndPartition, Long> internalOffsets = Arrays.stream(offsets)
                        .collect(Collectors.toMap(k -> k.topicAndPartition(), v -> v.fromOffset()));
                //log().info("commit Kafka Offsets {}", internalOffsets);
                thread.addAll(offsets);
            }
        };
        JavaDStream<ConsumerRecord<byte[], byte[]>> dStream = new JavaDStream<>(sylphKafkaOffset, ClassTag$.MODULE$.apply(ConsumerRecord.class));
        return dStream;
//        inputStream = inputStream.transform(rdd -> {
//            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//            Map<TopicAndPartition, Long> internalOffsets = Arrays.stream(offsets)
//                    .collect(Collectors.toMap(k -> k.topicAndPartition(), v -> v.fromOffset()));
//            commitKafkaOffsets(kafkaCluster, groupId, internalOffsets);
//            return rdd;
//        });
    }

    @Override
    public JavaDStream<Row> getSource()
    {
        return loadStream.get();
    }
}
