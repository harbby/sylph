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
package ideal.sylph.plugins.mysql;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.plugins.jdbc.JdbcRealTimeSink;

@Name("mysql")
@Description("this is mysql Sink, if table not execit ze create table")
public class MysqlSink
        extends JdbcRealTimeSink
{
    public MysqlSink(JdbcConfig mysqlConfig)
    {
        super(mysqlConfig);
    }

    @Override
    public String getJdbcDriver()
    {
        return "com.mysql.jdbc.Driver";
    }
}
