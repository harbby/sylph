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

import com.github.harbby.sylph.api.PluginConfig;
import com.github.harbby.sylph.api.Source;
import com.github.harbby.sylph.api.TableContext;
import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.api.annotation.Version;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Created by ideal on 17-4-25.
 */
@Name("socket")
@Version("1.0.0")
@Description("this spark socket source inputStream")
public class SocketSource
        implements Source<JavaDStream<Row>>
{
    private final transient JavaStreamingContext ssc;
    private final transient SocketSourceConfig config;
    private final transient TableContext context;

    public SocketSource(JavaStreamingContext ssc, SocketSourceConfig config, TableContext context)
    {
        this.ssc = ssc;
        this.config = config;
        this.context = context;
    }

    @Override
    public JavaDStream<Row> createSource()
    {
        String hosts = requireNonNull(config.hosts, "hosts is not setting");
        StructType schema = new StructType(new StructField[] {
                new StructField("host", StringType, true, Metadata.empty()),
                new StructField("port", StringType, true, Metadata.empty()),
                new StructField("value", StringType, true, Metadata.empty())});
        return Arrays.stream(hosts.split(","))
                .filter(x -> x.contains(":"))
                .collect(Collectors.toSet()).stream()
                .map(socket -> {
                    String[] split = socket.split(":");
                    JavaDStream<Row> socketSteam = ssc.socketTextStream(split[0], Integer.parseInt(split[1]))
                            .map(value -> new GenericRowWithSchema(new Object[] {split[0], Integer.parseInt(split[1]), value}, schema));
                    return socketSteam;
                }).reduce(JavaDStream::union).orElseThrow(() -> new IllegalArgumentException("hosts is is Empty"));
    }

    private static class SocketSourceConfig
            extends PluginConfig
    {
        private static final long serialVersionUID = 2L;
        @Name("socket_hosts")
        @Description("this is socket_hosts list")
        private String hosts = "localhost:9999";
    }
}
