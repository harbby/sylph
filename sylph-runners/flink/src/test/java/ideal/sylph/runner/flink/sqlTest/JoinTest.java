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

import ideal.sylph.etl.Collector;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.etl.join.JoinContext;
import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.runner.flink.sql.FlinkSqlParser;
import ideal.sylph.runner.flink.sqlTest.utils.PrintTableSink;
import ideal.sylph.runner.flink.table.SylphTableSource;
import ideal.sylph.runner.flink.udf.TimeUtil;
import ideal.sylph.spi.ConnectorStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkState;

/**
 * 经过研究 发现目前flin1.6 只支持流流join
 * <p>
 * tableEnv.registerTableSource("batch", new TestBatchTableSource());  //这里无法注册 说明目前flink1.6 还只支持流流 join
 */
public class JoinTest
{
    private StreamTableEnvironmentImpl tableEnv;
    private CreateTable dimTable;

    @Before
    public void init()
    {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(4);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        tableEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(execEnv);

        tableEnv.registerFunction("from_unixtime", new TimeUtil.FromUnixTime());

        //---create stream source
        TypeInformation[] fieldTypes = {Types.STRING(), Types.STRING(), Types.LONG()};
        String[] fieldNames = {"topic", "user_id", "time"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        DataStream<Row> dataSource = execEnv.fromCollection(new ArrayList<>(), rowTypeInfo);

        tableEnv.registerTableSource("tb1", new SylphTableSource(rowTypeInfo, dataSource));
        tableEnv.registerTableSource("tb0", new SylphTableSource(rowTypeInfo, dataSource));

        final AntlrSqlParser sqlParser = new AntlrSqlParser();
        this.dimTable = (CreateTable) sqlParser.createStatement(
                "create batch table users(id string, name string, city string) with(type = '"
                        + JoinOperator.class.getName() + "')");
    }

    @Test
    public void leftJoinTest()
            throws Exception
    {
        String leftJoin = "select tb1.*,users.* from tb1 left join users on tb1.user_id=users.id";

        FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                .setTableEnv(tableEnv)
                .setConnectorStore(ConnectorStore.getDefault())
                .build();

        flinkSqlParser.parser(leftJoin, ImmutableList.of(dimTable));
        System.out.println(tableEnv.execEnv().getExecutionPlan());
        tableEnv.execEnv().execute();
    }

    @Test
    public void moreLeftJoinTest()
            throws Exception
    {
        String leftJoin = "select tb1.*,users.name from tb1 left join users on tb1.user_id=users.id";
        String leftJoin2 = "select tb1.*,users.city from tb1 left join users on tb1.user_id=users.id";

        FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                .setTableEnv(tableEnv)
                .setConnectorStore(ConnectorStore.getDefault())
                .build();

        flinkSqlParser.parser(leftJoin, ImmutableList.of(dimTable));
        flinkSqlParser.parser(leftJoin2, ImmutableList.of(dimTable));
        System.out.println(tableEnv.execEnv().getExecutionPlan());
        tableEnv.execEnv().execute();
    }

    @Test
    public void leftJoinTest2()
    {
        String leftJoin = "select tb2.user_id as uid,tb2.*,users.* from " +
                "(select tb1.* from tb1 join users on tb1.user_id=users.id) as tb2 " +
                "left join users on tb2.user_id=users.id";

        FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                .setTableEnv(tableEnv)
                .setConnectorStore(ConnectorStore.getDefault())
                .build();

        flinkSqlParser.parser(leftJoin, ImmutableList.of(dimTable));

        String plan = tableEnv.execEnv().getExecutionPlan();
        System.out.println(plan);
        Assert.assertNotNull(plan);
    }

    @Test
    public void leftJoinTest3()
    {
        String leftJoin = "with tb2 as (select tb1.* from tb1 join users on tb1.user_id=users.id) select tb2.user_id as uid,tb2.*,users.* from tb2 left join users on tb2.user_id=users.id having user_id = 'uid_1' or uid is not null";

        FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                .setTableEnv(tableEnv)
                .setConnectorStore(ConnectorStore.getDefault())
                .build();

        flinkSqlParser.parser(leftJoin, ImmutableList.of(dimTable));

        String plan = tableEnv.execEnv().getExecutionPlan();
        System.out.println(plan);
        Assert.assertNotNull(plan);
    }

    @Test
    public void insetIntoTest4()
    {
        PrintTableSink printSink = new PrintTableSink();
        tableEnv.registerTableSink("sink1", printSink.getFieldNames(), printSink.getFieldTypes(), printSink);

        String leftJoin = "insert into sink1 with tb2 as (select tb1.* from tb1 join users on tb1.user_id=users.id) select tb2.user_id as uid,users.id,tb2.`time` from tb2 left join users on tb2.user_id=users.id";

        FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                .setTableEnv(tableEnv)
                .setConnectorStore(ConnectorStore.getDefault())
                .build();

        flinkSqlParser.parser(leftJoin, ImmutableList.of(dimTable));

        String plan = tableEnv.execEnv().getExecutionPlan();
        System.out.println(plan);
        Assert.assertNotNull(plan);
    }

    @Test
    public void leftJoinTest5()
    {
        String leftJoin = "select tb1.*,tb0.user_id,from_unixtime(tb0.`time`) from tb1 join tb0 on tb1.user_id = (tb0.user_id||'0') ";

        FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                .setTableEnv(tableEnv)
                .setConnectorStore(ConnectorStore.getDefault())
                .build();

        flinkSqlParser.parser(leftJoin, ImmutableList.of(dimTable));

        String plan = tableEnv.execEnv().getExecutionPlan();
        System.out.println(plan);
        Assert.assertNotNull(plan);
    }

    @Test
    public void easySqlTest()
    {
        String leftJoin = "select tb1.* from tb1";

        FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                .setTableEnv(tableEnv)
                .setConnectorStore(ConnectorStore.getDefault())
                .build();

        flinkSqlParser.parser(leftJoin, ImmutableList.of(dimTable));

        String plan = tableEnv.execEnv().getExecutionPlan();
        System.out.println(plan);
        Assert.assertNotNull(plan);
    }

    public static class JoinOperator
            implements RealTimeTransForm
    {
        public JoinOperator(JoinContext context)
        {
            checkState(context != null, "context is null");
        }

        @Override
        public void process(ideal.sylph.etl.Row input, Collector<ideal.sylph.etl.Row> collector)
        {
        }

        @Override
        public Schema getSchema()
        {
            return null;
        }

        @Override
        public boolean open(long partitionId, long version)
                throws Exception
        {
            return false;
        }

        @Override
        public void close(Throwable errorOrNull)
        {
        }
    }
}
