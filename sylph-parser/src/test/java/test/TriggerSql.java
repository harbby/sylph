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
package test;

import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.tree.Identifier;
import ideal.sylph.parser.antlr.tree.InsertInto;
import ideal.sylph.parser.antlr.tree.SelectQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TriggerSql
{
    private final AntlrSqlParser sqlParser = new AntlrSqlParser();

    @Test
    public void triggerSqlParser()
    {
        String sql = "SELECT\n" +
                "  id,\n" +
                "  TUMBLE_START(rowtime, INTERVAL '1' DAY) as start_time,\n" +
                "  COUNT(*) as cnt\n" +
                "FROM source\n" +
                "GROUP BY id, TUMBLE(rowtime, INTERVAL '1' DAY)\n" +
                "WINDOW LATE WITH DELAY '300' SECONDS\n" +
                "trigger WITH EVERY 60 SECONDS";

        SelectQuery selectQuery = (SelectQuery) sqlParser.createStatement(sql);
        Assert.assertTrue(selectQuery.getWindowTrigger().isPresent());
        Assert.assertEquals(selectQuery.getWindowTrigger().get().getCycleTime(), 60);
        Assert.assertEquals(selectQuery.getAllowedLateness().get().getAllowedLateness(), 300);
    }

    @Test
    public void triggerWithSqlParser()
    {
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
        InsertInto insertInto = (InsertInto) sqlParser.createStatement(sql);
        SelectQuery selectQuery = insertInto.getSelectQuery();
        Map.Entry<Identifier, SelectQuery> withQuery = selectQuery.getWithTableQuery().entrySet().iterator().next();
        Assert.assertEquals(withQuery.getKey().toString(), "t1");
        Assert.assertEquals(withQuery.getValue().getWindowTrigger().get().getCycleTime(), 5);
    }
}
