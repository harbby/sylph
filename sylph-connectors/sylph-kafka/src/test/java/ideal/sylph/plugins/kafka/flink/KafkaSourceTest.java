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

import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.runner.flink.engines.StreamSqlBuilder;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class KafkaSourceTest
{
    private static final AntlrSqlParser sqlParser = new AntlrSqlParser();

    public static StreamTableEnvironment getTableEnv()
    {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
        execEnv.setParallelism(2);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnv);
        return tableEnv;
    }

    @Test
    public void createSource()
            throws Exception
    {
        StreamTableEnvironment tableEnv = getTableEnv();
        String sql = "create input table tb1(\n" +
                "    _topic varchar,\n" +
                "    _message varchar\n" +
                ") with (\n" +
                "    type = '" + KafkaSource.class.getName() + "',\n" +
                "    kafka_topic = 'N603_A_1,N603_A_2,N603_A_3,N603_A_4,N603_A_5,N603_A_7',\n" +
                "    \"auto.offset.reset\" = latest,\n" +
                "    kafka_broker = 'localhost:9092',\n" +
                "    kafka_group_id = 'streamload1'\n" +
                ")";

        StreamSqlBuilder streamSqlBuilder = new StreamSqlBuilder(tableEnv, PipelinePluginManager.getDefault(), sqlParser);
        streamSqlBuilder.buildStreamBySql(sql);

        Table kafka = tableEnv.sqlQuery("select * from tb1");
        tableEnv.toAppendStream(kafka, Row.class).print();

        Assert.assertNotNull(tableEnv.execEnv().getStreamGraph().getJobGraph());
    }
}