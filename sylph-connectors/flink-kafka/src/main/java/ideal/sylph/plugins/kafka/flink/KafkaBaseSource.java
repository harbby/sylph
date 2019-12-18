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
package ideal.sylph.plugins.kafka.flink;

import com.github.harbby.gadtry.base.ArrayType;
import com.github.harbby.gadtry.base.JavaTypes;
import com.github.harbby.gadtry.base.MapType;
import ideal.sylph.etl.Field;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SourceContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static ideal.sylph.runner.flink.engines.StreamSqlUtil.schemaToRowTypeInfo;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public abstract class KafkaBaseSource
{
    private static final long serialVersionUID = 2L;
    private static final String[] KAFKA_COLUMNS = new String[] {"_topic", "_key", "_message", "_partition", "_offset"};

    public abstract FlinkKafkaConsumerBase<Row> getKafkaConsumerBase(List<String> topicSets,
            KafkaDeserializationSchema<Row> deserializationSchema, Properties properties);

    /**
     * 初始化(driver阶段执行)
     **/
    public DataStream<Row> createSource(StreamExecutionEnvironment execEnv, KafkaSourceConfig config, SourceContext context)
    {
        requireNonNull(execEnv, "execEnv is null");
        requireNonNull(config, "config is null");
        String topics = config.getTopics();
        String groupId = config.getGroupid();
        String offsetMode = config.getOffsetMode();

        Properties properties = new Properties();
        for (Map.Entry<String, Object> entry : config.getOtherConfig().entrySet()) {
            if (entry.getValue() != null) {
                properties.setProperty(entry.getKey(), entry.getValue().toString());
            }
        }

        properties.put("bootstrap.servers", config.getBrokers());  //需要把集群的host 配置到程序所在机器
        //"enable.auto.commit" -> (false: java.lang.Boolean), //不自动提交偏移量
        //      "session.timeout.ms" -> "30000", //session默认是30秒 超过5秒不提交offect就会报错
        //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期
        properties.put("group.id", groupId); //注意不同的流 group.id必须要不同 否则会出现offect commit提交失败的错误
        properties.put("auto.offset.reset", offsetMode); //latest   earliest

        KafkaDeserializationSchema<Row> deserializationSchema = "json".equals(config.getValueType()) ?
                new JsonDeserializationSchema(context.getSchema()) : new RowDeserializer();

        List<String> topicSets = Arrays.asList(topics.split(","));
        //org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
        FlinkKafkaConsumerBase<Row> base = getKafkaConsumerBase(topicSets, deserializationSchema, properties);
        return execEnv.addSource(base);
    }

    static class JsonDeserializationSchema
            implements KafkaDeserializationSchema<Row>
    {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private static TypeFactory typeFactory = TypeFactory.defaultInstance();

        private final RowTypeInfo rowTypeInfo;
        private final JsonDeserializer deserializer;
        private final Schema schema;

        public JsonDeserializationSchema(Schema schema)
        {
            this.schema = schema;
            this.rowTypeInfo = schemaToRowTypeInfo(schema);
            this.deserializer = new JsonDeserializer();
        }

        @Override
        public boolean isEndOfStream(Row nextElement)
        {
            return false;
        }

        @Override
        public Row deserialize(ConsumerRecord<byte[], byte[]> record)
        {
            deserializer.initNewMessage(record.value());

            Object[] values = new Object[schema.size()];
            for (int i = 0; i < schema.size(); i++) {
                Field field = schema.getField(i);
                switch (field.getName()) {
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
                        continue;
                }
                Object value = deserializer.deserialize(field);

                Type type = field.getJavaType();
                if (type instanceof MapType) {
                    MapType mapType = (MapType) type;

                    JavaType jType = typeFactory.constructMapType(mapType.getBaseClass(),
                            typeFactory.constructType(mapType.getKeyType()),
                            typeFactory.constructType(mapType.getValueType()));
                    value = MAPPER.convertValue(value, jType);
                }
                else if (type instanceof ArrayType) {
                    value = MAPPER.convertValue(value, typeFactory.constructType(((ArrayType) type).getValueType()));
                }
                else if (JavaTypes.typeToClass(type).isArray()) {
                    value = MAPPER.convertValue(value, JavaTypes.typeToClass(type));
                }
                else if (Number.class.isAssignableFrom(JavaTypes.typeToClass(type))) {
                    value = MAPPER.convertValue(value, JavaTypes.typeToClass(type));
                }
                values[i] = value;
            }
            return Row.of(values);
        }

        @Override
        public TypeInformation<Row> getProducedType()
        {
            return rowTypeInfo;
        }
    }

    private static class RowDeserializer
            implements KeyedDeserializationSchema<Row>
    {
        @Override
        public boolean isEndOfStream(Row nextElement)
        {
            return false;
        }

        @Override
        public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
        {
            return Row.of(
                    topic, //topic
                    messageKey == null ? null : new String(messageKey, UTF_8), //key
                    new String(message, UTF_8), //message
                    partition,
                    offset
            );
        }

        @Override
        public TypeInformation<Row> getProducedType()
        {
            TypeInformation<?>[] types = new TypeInformation<?>[] {
                    TypeExtractor.createTypeInfo(String.class),
                    TypeExtractor.createTypeInfo(String.class), //createTypeInformation[String]
                    TypeExtractor.createTypeInfo(String.class),
                    Types.INT,
                    Types.LONG
            };
            return new RowTypeInfo(types, KAFKA_COLUMNS);
        }
    }
}
