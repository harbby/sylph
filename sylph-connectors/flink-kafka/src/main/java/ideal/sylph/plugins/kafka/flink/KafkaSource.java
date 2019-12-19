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
import ideal.sylph.etl.SourceContext;
import ideal.sylph.etl.api.Source;
import org.apache.flink.shaded.guava18.com.google.common.base.Supplier;
import org.apache.flink.shaded.guava18.com.google.common.base.Suppliers;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Properties;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

@Name(value = "kafka")
@Version("1.0.0")
@Description("this flink kafka source inputStream")
public class KafkaSource
        extends KafkaBaseSource
        implements Source<DataStream<Row>>
{
    private static final long serialVersionUID = 2L;
    private final transient Supplier<DataStream<Row>> loadStream;

    /**
     * 初始化(driver阶段执行)
     **/
    public KafkaSource(StreamExecutionEnvironment execEnv, KafkaSourceConfig config, SourceContext context)
    {
        requireNonNull(execEnv, "execEnv is null");
        requireNonNull(config, "config is null");
        loadStream = Suppliers.memoize(() -> this.createSource(execEnv, config, context));

        checkState(!"largest".equals(config.getOffsetMode()), "kafka 0.10+, use latest");
        checkState(!"smallest".equals(config.getOffsetMode()), "kafka 0.10+, use earliest");
    }

    @Override
    public FlinkKafkaConsumerBase<Row> getKafkaConsumerBase(List<String> topicSets, KafkaDeserializationSchema<Row> deserializationSchema, Properties properties)
    {
        //"enable.auto.commit"-> true
        //"auto.commit.interval.ms" -> 90000
        return new FlinkKafkaConsumer010<>(
                topicSets,
                deserializationSchema,
                properties);
    }

    @Override
    public DataStream<Row> getSource()
    {
        return loadStream.get();
    }
}
