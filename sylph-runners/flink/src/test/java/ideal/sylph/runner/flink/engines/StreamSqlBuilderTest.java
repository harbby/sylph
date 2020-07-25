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
package ideal.sylph.runner.flink.engines;

import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.runner.flink.resource.PrintTableSink;
import ideal.sylph.runner.flink.resource.TestTableSource;
import ideal.sylph.spi.ConnectorStore;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

public class StreamSqlBuilderTest
{
    private final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
    private final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(execEnv);
    private final StreamSqlBuilder streamSqlBuilder = new StreamSqlBuilder(tableEnv, ConnectorStore.getDefault(), new AntlrSqlParser());

    private void registerDataStream()
    {
        TestTableSource tableSource = new TestTableSource();
        tableEnv.registerDataStream("tb0", tableSource.getDataStream(execEnv),
                String.join(",", tableSource.getTableSchema().getFieldNames()) + ",proctime.proctime");
    }

    private void registerOneRowDataStream()
    {
        Table source = tableEnv.sqlQuery("select 'uid_6' as user_id, 'topic_1' as topic, " + System.currentTimeMillis() + " as `time`");
        tableEnv.registerDataStream("tb0", tableEnv.toAppendStream(source, Row.class).map(x -> x).returns(source.getSchema().toRowType()),
                String.join(",", source.getSchema().getFieldNames()) + ",proctime.proctime");
    }

    @Test
    public void windowSqlTrigger()
            throws Exception
    {
        registerOneRowDataStream();
        tableEnv.registerTableSink("sink1", new PrintTableSink());

        String sql = "insert into sink1 \n" +
                "SELECT\n" +
                "  user_id,\n" +
                "  cast(TUMBLE_START(proctime, INTERVAL '1' DAY) as varchar) as start_time,\n" +
                "  COUNT(*) as cnt\n" +
                "FROM tb0\n" +
                "GROUP BY user_id, TUMBLE(proctime, INTERVAL '1' DAY)\n" +
                "trigger WITH EVERY 5 SECONDS";

        streamSqlBuilder.buildStreamBySql(sql);
        execEnv.execute();
    }

    @Test
    public void windowWithSqlTrigger()
            throws Exception
    {
        registerOneRowDataStream();
        tableEnv.registerTableSink("sink1", new PrintTableSink());

        String sql = "insert into sink1 \n" +
                "with t1 as (\n" +
                "SELECT\n" +
                "  user_id,\n" +
                "  cast(TUMBLE_START(proctime, INTERVAL '1' DAY) as varchar) as start_time,\n" +
                "  COUNT(*) as cnt\n" +
                "FROM tb0\n" +
                "GROUP BY user_id, TUMBLE(proctime, INTERVAL '1' DAY)\n" +
                "trigger WITH EVERY 5 SECONDS)\n" +
                "select * from t1";

        streamSqlBuilder.buildStreamBySql(sql);
        execEnv.execute();
    }

    @Test
    public void globalWindowSqlTrigger()
            throws Exception
    {
        registerOneRowDataStream();
        tableEnv.registerTableSink("sink1", new PrintTableSink());

        String sql = "insert into sink1 \n" +
                "SELECT\n" +
                "  user_id,\n" +
                "  'global',\n" +
                "  COUNT(*) as cnt\n" +
                "FROM tb0\n" +
                "GROUP BY user_id";

        streamSqlBuilder.buildStreamBySql(sql);
        execEnv.execute();
    }

    @Test
    public void globalWindowSelectSqlTrigger()
            throws Exception
    {
        registerOneRowDataStream();
        tableEnv.registerTableSink("sink1", new PrintTableSink());

        String sql = "SELECT\n" +
                "  user_id,\n" +
                "  'global',\n" +
                "  COUNT(*) as cnt\n" +
                "FROM tb0\n" +
                "GROUP BY user_id";

        streamSqlBuilder.buildStreamBySql(sql);
        execEnv.execute();
    }
}