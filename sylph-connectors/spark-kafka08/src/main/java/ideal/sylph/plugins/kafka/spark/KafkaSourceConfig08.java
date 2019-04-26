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

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;

public class KafkaSourceConfig08
        extends PluginConfig
{
    private static final long serialVersionUID = 2L;

    @Name("kafka_topic")
    @Description("this is kafka topic list")
    private String topics = "test1";

    @Name("kafka_broker")
    @Description("this is kafka broker list")
    private String brokers = "localhost:9092";

    @Name("kafka_group_id")
    @Description("this is kafka_group_id")
    private String groupid = "sylph_streamSql_test1";

    @Name("auto.offset.reset")
    @Description("this is auto.offset.reset mode")
    private String offsetMode = "latest";

    @Name("value_type")
    @Description("this is kafka String value Type, use json")
    private String valueType;

    public String getTopics()
    {
        return topics;
    }

    public String getBrokers()
    {
        return brokers;
    }

    public String getGroupid()
    {
        return groupid;
    }

    public String getOffsetMode()
    {
        return offsetMode;
    }

    public String getValueType()
    {
        return valueType;
    }

    private KafkaSourceConfig08() {}
}
