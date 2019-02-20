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

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.etl.api.Source;
import ideal.sylph.plugins.kafka.flink.utils.JsonSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.calcite.shaded.com.google.common.base.Supplier;
import org.apache.flink.calcite.shaded.com.google.common.base.Suppliers;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Name(value = "kafka09")
@Version("1.0.0")
@Description("this flink kafka source inputStream")
public class KafkaSource09
        implements Source<DataStream<Row>>
{
    private static final long serialVersionUID = 2L;
    private static final String[] KAFKA_COLUMNS = new String[] {"_topic", "_key", "_message", "_partition", "_offset"};

    private final transient Supplier<DataStream<Row>> loadStream;

    /**
     * 初始化(driver执行)
     **/
    public KafkaSource09(StreamTableEnvironment tableEnv, KafkaSource09Config config, SourceContext context)
    {
        requireNonNull(tableEnv, "tableEnv is null");
        requireNonNull(config, "config is null");
        loadStream = Suppliers.memoize(() -> {
            String topics = config.topics;

            Properties properties = new Properties();
            properties.put("bootstrap.servers", config.brokers);  //需要注意hosts问题
            //"enable.auto.commit" -> (false: java.lang.Boolean), //不自动提交偏移量
            //      "session.timeout.ms" -> "30000", //session默认是30秒 超过5秒不提交offect就会报错
            //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期
            properties.put("group.id", config.groupid); //注意不同的流 group.id必须要不同 否则会出现offect commit提交失败的错误
            properties.put("auto.offset.reset", config.offsetMode); //latest   earliest
            properties.put("zookeeper.connect", config.zookeeper);

            List<String> topicSets = Arrays.asList(topics.split(","));
            //org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
            DataStream<Row> stream = tableEnv.execEnv().addSource(new FlinkKafkaConsumer09<Row>(
                    topicSets,
                    new JsonSchema(context),
                    properties)
            );
            return stream;
        });
    }

    @Override
    public DataStream<Row> getSource()
    {
        return loadStream.get();
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

    public static class KafkaSource09Config
            extends PluginConfig
    {
        private static final long serialVersionUID = 2L;

        @Name("kafka_topic")
        @Description("this is kafka topic list")
        private String topics = "test1";

        @Name("kafka_broker")
        @Description("this is kafka broker list")
        private String brokers = "localhost:9092";

        @Name("zookeeper.connect")
        @Description("this is kafka zk list")
        private String zookeeper = "localhost:2181";

        @Name("kafka_group_id")
        @Description("this is kafka_group_id")
        private String groupid = "sylph_streamSql_test1";

        @Name("auto.offset.reset")
        @Description("this is auto.offset.reset mode")
        private String offsetMode = "latest";

        private KafkaSource09Config() {}
    }
}
