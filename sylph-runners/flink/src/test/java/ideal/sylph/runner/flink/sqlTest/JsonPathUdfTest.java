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

import ideal.sylph.runner.flink.udf.UDFJson;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JsonPathUdfTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ConcurrentMap<String, String> result = new ConcurrentHashMap<>();

    private StreamTableEnvironment tableEnv;
    private Table table;

    @Before
    public void init()
            throws JsonProcessingException
    {
        String json = MAPPER.writeValueAsString(ImmutableMap.of("user_id", "uid_001",
                "ip", "127.0.0.1",
                "store", 12.0,
                "key1", ImmutableMap.of("key2", 123)
        ));

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
        execEnv.setParallelism(2);
        tableEnv = TableEnvironment.getTableEnvironment(execEnv);
        tableEnv.registerFunction("get_json_object", new UDFJson());
        table = tableEnv.sqlQuery("select '" + json + "' as message");
    }

    @Test
    public void jsonPathUdfTestReturn123()
            throws Exception
    {

        String jsonKey = "$.key1.key2";
        //Table table = tableEnv.sqlQuery("select cast(json['store'] as double) from tp , LATERAL TABLE(json_parser(message, 'store', 'ip')) as T(json) ");
        Table table1 = tableEnv.sqlQuery("select get_json_object(message,'" + jsonKey + "') from " + table);
        tableEnv.toAppendStream(table1, Row.class)
                .addSink(new SinkFunction<Row>()
                {
                    @Override
                    public void invoke(Row value, Context context)
                            throws Exception
                    {
                        result.put(jsonKey, (String) value.getField(0));
                    }
                });
        tableEnv.execEnv().execute();
        Assert.assertEquals("123", result.get(jsonKey));
    }

    @Test
    public void jsonPathUdfTest()
            throws Exception
    {
        String jsonKey = "$.key2.key2";
        result.put(jsonKey, "ok");
        Table table1 = tableEnv.sqlQuery("select get_json_object(message,'" + jsonKey + "') from " + table);
        tableEnv.toAppendStream(table1, Row.class)
                .addSink(new SinkFunction<Row>()
                {
                    @Override
                    public void invoke(Row value, Context context)
                            throws Exception
                    {
                        if (value.getField(0) == null) {
                            result.remove(jsonKey);
                        }
                    }
                });
        tableEnv.execEnv().execute();
        Assert.assertNull(result.get(jsonKey));
    }
}
