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
package ideal.sylph.runner.flink.resource;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.api.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.types.Row;

@Name("testSource")
@Description("test source")
public class SylphTestSource
        implements Source<DataStream<Row>>
{
    private final transient StreamTableEnvironment tableEnv;
    private final transient StreamExecutionEnvironment execEnv;

    public SylphTestSource(StreamTableEnvironment tableEnv)
    {
        this.tableEnv = tableEnv;
        this.execEnv = ((StreamTableEnvironmentImpl) tableEnv).execEnv();
    }

    @Override
    public DataStream<Row> getSource()
    {
//        Table table = sess.fromTableSource(new TestTableSource());
//        table.printSchema();
//        return sess.toAppendStream(table, Row.class);
        return new TestTableSource().getDataStream(execEnv);
    }
}
