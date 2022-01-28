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

import com.github.harbby.sylph.api.Source;
import com.github.harbby.sylph.api.TableContext;
import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.api.annotation.Version;
import com.github.harbby.sylph.json.JsonReadCodeGenerator;
import ideal.sylph.plugins.kafka.flink.runtime.JsonDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkState;

@Name(value = "kafka")
@Version("1.0.0")
@Description("this flink kafka source inputStream")
public class KafkaSource
        implements Source<DataStream<Row>>
{
    private static final List<String> KAFKA_COLUMNS = Arrays.asList("_topic", "_key", "_message", "_partition", "_offset");
    private static final RowTypeInfo ROW_TYPE_INFO = new RowTypeInfo(
            new TypeInformation<?>[] {Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.LONG},
            KAFKA_COLUMNS.toArray(new String[0]));
    private static final long serialVersionUID = 4494404815203109735L;

    private final transient StreamExecutionEnvironment execEnv;
    private final transient KafkaSourceConfig config;
    private final transient TableContext context;

    public KafkaSource(StreamExecutionEnvironment execEnv, KafkaSourceConfig config, TableContext context)
    {
        this.execEnv = requireNonNull(execEnv, "execEnv is null");
        this.config = requireNonNull(config, "config is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public DataStream<Row> createSource()
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
        properties.put("bootstrap.servers", config.getBrokers());
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", offsetMode);
        List<String> topicSets = Arrays.asList(topics.split(","));

        checkState("json".equalsIgnoreCase(config.getValueType()), "only support json message");
        JsonReadCodeGenerator codeGenerator = new JsonReadCodeGenerator("JsonCodeGenReader");
        codeGenerator.doCodeGen(this.context.getSchema());

        KafkaDeserializationSchema<Row> kafkaDeserializationSchema = new JsonDeserializationSchema(
                context.getSchema(),
                codeGenerator.getFullName(),
                codeGenerator.getByteCode(),
                codeGenerator.getCode());
        return execEnv.addSource(new FlinkKafkaConsumer<>(topicSets, kafkaDeserializationSchema, properties));
    }
}
