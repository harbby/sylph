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

import com.github.harbby.sylph.api.Schema;
import com.github.harbby.sylph.api.Source;
import com.github.harbby.sylph.api.TableContext;
import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.api.annotation.Version;
import com.github.harbby.sylph.json.ByteCodeClassLoader;
import com.github.harbby.sylph.json.JsonPathReader;
import com.github.harbby.sylph.json.JsonReadCodeGenerator;
import com.github.harbby.sylph.json.KafkaRecord;
import com.github.harbby.sylph.runner.spark.kafka.SylphKafkaOffset;
import com.github.harbby.sylph.runner.spark.sparkstreaming.DStreamUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import scala.Serializable;
import scala.reflect.ClassTag$;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;

@Name("kafka")
@Version("1.0.0")
@Description("this spark kafka 0.10+ source inputStream")
public class KafkaSource
        implements Source<JavaDStream<Row>>
{
    private final transient JavaStreamingContext ssc;
    private final transient KafkaSourceConfig config;
    private final transient TableContext context;

    public KafkaSource(JavaStreamingContext ssc, KafkaSourceConfig config, TableContext context)
    {
        this.ssc = ssc;
        this.config = config;
        this.context = context;
    }

    @Override
    public JavaDStream<Row> createSource()
    {
        String topics = config.getTopics();
        String brokers = config.getBrokers();
        String groupId = config.getGroupid();
        String offsetMode = config.getOffsetMode();

        Map<String, Object> kafkaParams = new HashMap<>(config.getOtherConfig());
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class); //StringDeserializer
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class); //StringDeserializer
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", offsetMode); //latest or earliest

        List<String> topicSets = Arrays.asList(topics.split(","));
        checkState("json".equalsIgnoreCase(config.getValueType()), "only support json message");

        JavaDStream<ConsumerRecord<byte[], byte[]>> jDStream = createDStream(ssc, topicSets, kafkaParams);
        //StructType rowTypeInfo = schemaToSparkType(schema);
        Schema schema = context.getSchema();
        JsonReadCodeGenerator codeGenerator = new JsonReadCodeGenerator("JsonCodeGenReader");
        codeGenerator.doCodeGen(schema);
        StreamingJsonReaderMap jsonReaderMap = new StreamingJsonReaderMap(codeGenerator.getFullName(),
                codeGenerator.getByteCode(), schema.size());
        return jDStream.map(jsonReaderMap).filter(Objects::nonNull);
    }

    private static JavaDStream<ConsumerRecord<byte[], byte[]>> createDStream(JavaStreamingContext ssc, List<String> topicSets, Map<String, Object> kafkaParams)
    {
        JavaInputDStream<ConsumerRecord<byte[], byte[]>> inputStream = KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicSets, kafkaParams));
        DStream<ConsumerRecord<byte[], byte[]>> sylphKafkaOffset = new SylphKafkaOffset<ConsumerRecord<byte[], byte[]>>(inputStream.inputDStream())
        {
            private static final long serialVersionUID = -2021749056188222072L;

            @Override
            public void commitOffsets(RDD<?> kafkaRdd)
            {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) kafkaRdd).offsetRanges();
                log().info("commitKafkaOffsets {}", (Object) offsetRanges);
                DStream<?> firstDStream = DStreamUtil.getFirstDStream(inputStream.dstream());
                ((CanCommitOffsets) firstDStream).commitAsync(offsetRanges);
            }
        };
        return new JavaDStream<>(sylphKafkaOffset, ClassTag$.MODULE$.apply(ConsumerRecord.class));
    }

    public static class StreamingJsonReaderMap
            implements Function<ConsumerRecord<byte[], byte[]>, Row>, Serializable
    {
        private static final long serialVersionUID = -192589774321712777L;
        private final String classFullName;
        private final byte[] byteCode;
        private transient JsonPathReader jsonPathReader;
        private transient KafkaRecordWrapper kafkaRecordWrapper;
        private final int columnSize;

        public StreamingJsonReaderMap(String classFullName, byte[] byteCode, int columnSize)
        {
            this.classFullName = classFullName;
            this.byteCode = byteCode;
            this.columnSize = columnSize;
        }

        @Override
        public Row call(ConsumerRecord<byte[], byte[]> record)
                throws Exception
        {
            byte[] message = record.value();
            if (message == null) {
                return null;
            }
            if (jsonPathReader == null) {
                this.jsonPathReader = createJsonDeserializer();
                this.kafkaRecordWrapper = new KafkaRecordWrapper();
            }
            jsonPathReader.initNewMessage(message);
            Object[] values = new Object[columnSize];
            kafkaRecordWrapper.record = record;
            jsonPathReader.deserialize(kafkaRecordWrapper, values);
            return new GenericRow(values);
        }

        private JsonPathReader createJsonDeserializer()
        {
            ByteCodeClassLoader loader = new ByteCodeClassLoader(Thread.currentThread().getContextClassLoader());
            Class<? extends JsonPathReader> codeGenClass = loader.defineClass(classFullName, byteCode).asSubclass(JsonPathReader.class);
            try {
                return codeGenClass.getConstructor().newInstance();
            }
            catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new IllegalStateException("kafka code gen error");
            }
        }
    }

    public static class KafkaRecordWrapper
            extends KafkaRecord<byte[], byte[]>
    {
        private ConsumerRecord<byte[], byte[]> record;

        @Override
        public String topic()
        {
            return record.topic();
        }

        @Override
        public Integer partition()
        {
            return record.partition();
        }

        @Override
        public byte[] key()
        {
            return record.key();
        }

        @Override
        public byte[] value()
        {
            return record.value();
        }

        @Override
        public Long offset()
        {
            return record.offset();
        }
    }
}
