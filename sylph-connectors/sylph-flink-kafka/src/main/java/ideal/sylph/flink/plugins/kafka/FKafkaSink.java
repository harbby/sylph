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
package ideal.sylph.flink.plugins.kafka;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.etl.api.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Name("fkfksink")
@Description("this is kafkaSink plugin")
public class FKafkaSink
        implements Sink<DataStream<Row>>
{
    private static final Logger logger = LoggerFactory.getLogger(FKafkaSink.class);
    private final ideal.sylph.etl.Row.Schema schema;
    private final FKafka09SinkConfig config;
    private final SourceContext cxt;

    public FKafkaSink(FKafka09SinkConfig config, SourceContext context){
       this.schema=context.getSchema();
       this.config=config;
       this.cxt=context;
    }

    @Override
    public void run(DataStream<Row> stream) {

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
