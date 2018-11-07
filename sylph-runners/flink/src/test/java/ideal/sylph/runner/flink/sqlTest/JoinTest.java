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
package ideal.sylph.runner.flink.sqlTest;

import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.runner.flink.sql.FlinkSqlParser;
import ideal.sylph.runner.flink.sqlTest.utils.PrintTableSink;
import ideal.sylph.runner.flink.sqlTest.utils.TestTableSource;
import ideal.sylph.runner.flink.udf.TimeUtil;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * 经过研究 发现目前flin1.6 只支持流流join
 * <p>
 * tableEnv.registerTableSource("batch", new TestBatchTableSource());  //这里无法注册 说明目前flink1.6 还只支持流流 join
 */
public class JoinTest
{
    private StreamTableEnvironment tableEnv;

    @Before
    public void init()
    {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(2);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        tableEnv = TableEnvironment.getTableEnvironment(execEnv);
    }

    @Test
    public void appendStreamTest()
            throws Exception
    {
        Table table = tableEnv.sqlQuery("SELECT * FROM (VALUES ('Bob'), ('Bob')) AS NameTable(name)");  //这个例子是append模式
        tableEnv.toAppendStream(table, Row.class).print();
        Assert.assertTrue(true);
        //tableEnv.execEnv().execute();
    }

    @Test
    public void RetractStreamTest()
            throws Exception
    {
        //--- no keyBy group is toRetractStream mode
        // this is global window
        Table table = tableEnv.sqlQuery("SELECT name, count(1) FROM (VALUES ('Bob'), ('Bob')) AS NameTable(name) GROUP BY name");
        Assert.assertNotNull(tableEnv.toRetractStream(table, Row.class).print());

        //tableEnv.execEnv().execute();
    }

    @Test
    public void joinTest()
            throws Exception
    {
        final AntlrSqlParser sqlParser = new AntlrSqlParser();
        CreateTable createTable = (CreateTable) sqlParser.createStatement("create batch table users(id string, name string, city string) with(type = 'ideal.sylph.plugins.mysql.MysqlAsyncFunction')");

        List<String> querys = ImmutableList.<String>builder()
                .add("select tb1.*,users.* from tb1 left join users on tb1.user_id=users.id")
                .add("select tb2.user_id as uid,tb2.*,users.* from (select tb1.* from tb1 join users on tb1.user_id=users.id) as tb2 left join users on tb2.user_id=users.id")
                .add("with tb2 as (select tb1.* from tb1 join users on tb1.user_id=users.id) select tb2.user_id as uid,tb2.*,users.* from tb2 left join users on tb2.user_id=users.id having user_id = 'uid_1' or uid is not null")
                .add("insert into sink1 with tb2 as (select tb1.* from tb1 join users on tb1.user_id=users.id) select tb2.user_id as uid,users.id,tb2.`time` from tb2 left join users on tb2.user_id=users.id")
                .add("select tb1.*,tb0.user_id,from_unixtime(tb0.`time`) from tb1 join tb0 on tb1.user_id = (tb0.user_id||'0') ")
                .add("select tb1.* from tb1 ")
                .build();

        for (String query : querys) {
            tableEnv = TableEnvironment.getTableEnvironment(tableEnv.execEnv());
            tableEnv.registerFunction("from_unixtime", new TimeUtil.FromUnixTime());
            tableEnv.execEnv().setParallelism(4);

            TableSource<Row> tableSource = new TestTableSource();
            tableEnv.registerTableSource("tb1", tableSource);
            tableEnv.registerTableSource("tb0", new TestTableSource());

            PrintTableSink printSink = new PrintTableSink("/path/to/file");
            tableEnv.registerTableSink("sink1", printSink.getFieldNames(), printSink.getFieldTypes(), printSink);

            FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                    .setTableEnv(tableEnv)
                    .setBatchPluginManager(PipelinePluginManager.getDefault())
                    .build();
            flinkSqlParser.parser(query, ImmutableList.of(createTable));

            String plan = tableEnv.execEnv().getExecutionPlan();
            System.out.println(plan);
            Assert.assertNotNull(plan);
            //tableEnv.execEnv().execute();
        }
    }
}
