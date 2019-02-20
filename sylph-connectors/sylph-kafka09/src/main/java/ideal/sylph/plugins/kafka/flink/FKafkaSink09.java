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
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.etl.api.Sink;
import ideal.sylph.plugins.kafka.flink.utils.JsonSchema;
import ideal.sylph.plugins.kafka.flink.utils.KafkaProducer;
import ideal.sylph.plugins.kafka.flink.utils.SeriSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkState;

@Name("fkfk09sink")
@Description("this is kafka09 Sink plugin")
public class FKafkaSink09
        implements Sink<DataStream<Row>>
{
    private static final Logger logger = LoggerFactory.getLogger(FKafkaSink09.class);
    private final ideal.sylph.etl.Row.Schema schema;
    private final FKafka09SinkConfig config;
    private final SourceContext cxt;

    public FKafkaSink09(FKafka09SinkConfig config, SourceContext context){
       this.schema=context.getSchema();
       this.config=config;
       this.cxt=context;
    }

    @Override
    public void run(DataStream<Row> stream) {
        FlinkKafkaProducer09<Row> producer09 = new FlinkKafkaProducer09<Row>(
                config.brokers,
                config.topics,
                new JsonSchema(cxt));
    }

    public static class FKafka09SinkConfig
            extends PluginConfig
    {
        private static final long serialVersionUID = 2L;
        @Name("kafka_topic")
        @Description("this is kafka topic list")
        private String topics;

        @Name("kafka_broker")
        @Description("this is kafka broker list")
        private String brokers = "localhost:6667";

    }
}
