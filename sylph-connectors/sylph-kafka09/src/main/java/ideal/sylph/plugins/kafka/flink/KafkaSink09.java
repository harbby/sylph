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

import com.google.gson.Gson;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.plugins.kafka.flink.utils.KafkaProducer;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkState;

@Name("kafka09")
@Description("this is kafka09 Sink plugin")
public class KafkaSink09
        implements RealTimeSink
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink09.class);
    private final Kafka09SinkConfig config;
    private final Row.Schema schema;
    private int idIndex = -1;
    private KafkaProducer kafkaProducer;
    private final String topic;

    public KafkaSink09(SinkContext context, Kafka09SinkConfig config)
    {
        schema = context.getSchema();
        if (!Strings.isNullOrEmpty(config.idField)) {
            int fieldIndex = schema.getFieldIndex(config.idField);
            checkState(fieldIndex != -1, config.idField + " does not exist, only " + schema.getFields());
            this.idIndex = fieldIndex;
        }
        this.config = config;
        this.topic = config.topics;
    }

    @Override
    public void process(Row value)
    {
        Gson gson = new Gson();
        Map<String, Object> map = new HashMap<>();
        for (String fieldName : schema.getFieldNames()) {
            map.put(fieldName, value.getAs(fieldName));
        }
        String message = gson.toJson(map);
        kafkaProducer.send(message);
    }

    @Override
    public boolean open(long partitionId, long version)
            throws Exception
    {
        //config.zookeeper,config.brokers  至少一个  暂时 zookeeper
        this.kafkaProducer = new KafkaProducer(config.zookeeper, config.topics);
        return true;
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        kafkaProducer.close();
    }

    public static class Kafka09SinkConfig
            extends PluginConfig
    {
        private static final long serialVersionUID = 2L;
        @Name("kafka_topic")
        @Description("this is kafka topic list")
        private String topics;

        @Name("kafka_broker")
        @Description("this is kafka broker list")
        private String brokers = "localhost:6667";

        @Name("zookeeper.connect")
        @Description("this is kafka zk list")
        private String zookeeper;

        @Name("id_field")
        @Description("this is kafka id_field")
        private String idField;
    }
}
