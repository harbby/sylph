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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class TableSqlTest
{
    public static StreamTableEnvironment getTableEnv()
    {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
        execEnv.setParallelism(2);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(execEnv);
        return tableEnv;
    }

    @Test
    public void selectNullThrowsException()
    {
        StreamTableEnvironment tableEnv = getTableEnv();
        try {
            tableEnv.toAppendStream(tableEnv.sqlQuery("select null"), Row.class).print();
            Assert.fail();
        }
        catch (ValidationException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void selectNullTest()
            throws Exception
    {
        StreamTableEnvironment tableEnv = getTableEnv();

        tableEnv.toAppendStream(tableEnv.sqlQuery("select cast(null as varchar) as a1"), Row.class).print();
        tableEnv.execute("");
    }

    @Test
    public void selectLocalTimeTest()
            throws Exception
    {
        StreamTableEnvironment tableEnv = getTableEnv();

        tableEnv.toAppendStream(tableEnv.sqlQuery("select LOCALTIMESTAMP as `check_time`"), Row.class).print();
        tableEnv.execute("");
    }

//    @Test
//    public void mapSizeTest()
//            throws Exception
//    {
//        StreamTableEnvironment tableEnv = getTableEnv();
//
//        Schema schema = Schema.newBuilder()
//                .add("props", JavaTypes.make(Map.class, new Class[] {String.class, Integer.class}, null))
//                .build();
//        RowTypeInfo rowTypeInfo = schemaToRowTypeInfo(schema);
//        Map<String, Integer> mapValue = new HashMap<>();
//        mapValue.put("key", 123);
//
//        DataStream<Row> stream = tableEnv.execEnv().fromElements(Row.of(mapValue)).returns(rowTypeInfo);
//        Table table = tableEnv.fromDataStream(stream, "props");
//
//        table.printSchema();
//        tableEnv.toAppendStream(tableEnv.sqlQuery("select size(props) from " + table), Row.class).print();
//
//        Assert.assertNotNull(tableEnv.execEnv().getExecutionPlan());
//    }

    @Test
    public void selectCastErrorTest()
            throws Exception
    {
        StreamTableEnvironmentImpl tableEnv = (StreamTableEnvironmentImpl) getTableEnv();

        DataStreamSource<Row> stream = tableEnv.execEnv().fromElements(Row.of("a1", "3.14"));
        Table table = tableEnv.fromDataStream(stream, "name,age");

        tableEnv.toAppendStream(tableEnv.sqlQuery("select name,cast(age as bigint) as a1 from " + table), Row.class)
                .addSink(new RichSinkFunction<Row>()
                {
                    @Override
                    public void invoke(Row value, Context context)
                            throws Exception
                    {
                        long a1 = (long) value.getField(0);
                        System.out.println(value);
                    }
                });
        try {
            tableEnv.execute("");
            Assert.fail();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void selectSinkErrorTest()
            throws Exception
    {
        StreamTableEnvironmentImpl tableEnv = (StreamTableEnvironmentImpl) getTableEnv();

        DataStreamSource<Row> stream = tableEnv.execEnv().fromElements(Row.of("a1", "3.14"));
        Table table = tableEnv.fromDataStream(stream, "name,age");
        tableEnv.toAppendStream(tableEnv.sqlQuery("select name,age from " + table), Row.class)
                .addSink(new RichSinkFunction<Row>()
                {
                    @Override
                    public void invoke(Row value, Context context)
                            throws Exception
                    {
                        long a1 = (long) value.getField(1);
                        System.out.println(value);
                    }
                });
        try {
            tableEnv.execute("");
            Assert.fail();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
