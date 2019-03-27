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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestStreamMode
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
    public void toAppendStreamTest()
            throws Exception
    {
        Table table = tableEnv.sqlQuery("SELECT * FROM (VALUES ('Bob'), ('Bob')) AS NameTable(name)");  //这个例子是append模式
        tableEnv.toAppendStream(table, Row.class).print();
        Assert.assertTrue(true);
        //tableEnv.execEnv().execute();
    }

    @Test
    public void toRetractStreamTest()
            throws Exception
    {
        //--- no keyBy group is toRetractStream mode
        // this is global window
        Table table = tableEnv.sqlQuery("SELECT name, count(1) FROM (VALUES ('Bob'), ('Bob')) AS NameTable(name) GROUP BY name");
        Assert.assertNotNull(tableEnv.toRetractStream(table, Row.class).print());

        //tableEnv.execEnv().execute();
    }
}
