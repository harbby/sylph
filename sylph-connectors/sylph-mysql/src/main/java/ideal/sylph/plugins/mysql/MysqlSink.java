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
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.api.RealTimeSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkState;

@Name("mysql")
@Description("this is mysql Sink, if table not execit ze create table")
public class MysqlSink
        implements RealTimeSink
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlSink.class);

    private final MysqlConfig config;

    private Connection connection;
    private PreparedStatement statement;
    private int num = 0;
    private final String prepareStatementQuery;
    private final String[] keys;

    public MysqlSink(MysqlConfig mysqlConfig)
    {
        this.config = mysqlConfig;
        checkState(config.getQuery() != null, "insert into query not setting");
        this.prepareStatementQuery = config.getQuery().replaceAll("\\$\\{.*?}", "?");
        // parser sql query ${key}
        Matcher matcher = Pattern.compile("(?<=\\$\\{)(.+?)(?=\\})").matcher(config.getQuery());
        List<String> builder = new ArrayList<>();
        while (matcher.find()) {
            builder.add(matcher.group());
        }
        this.keys = builder.toArray(new String[0]);

        checkMysql();
    }

    private void checkMysql()
    {
        try {
            this.open(0, 9);
        }
        finally {
            this.close(null);
        }
    }

    @Override
    public boolean open(long partitionId, long version)
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.connection = DriverManager.getConnection(config.jdbcUrl, config.user, config.password);
            this.statement = connection.prepareStatement(prepareStatementQuery);
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("Mysql connection open fail", e);
        }
        return true;
    }

    @Override
    public void process(Row row)
    {
        try {
            int i = 1;
            for (String key : keys) {
                Object value = isNumeric(key) ? row.getAs(Integer.parseInt(key)) : row.getAs(key);
                statement.setObject(i, value);
                i += 1;
            }
            statement.addBatch();
            // submit batch
            if (num++ >= 50) {
                statement.executeBatch();
                num = 0;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        try (Connection conn = connection) {
            try (Statement stmt = statement) {
                if (stmt != null) {
                    stmt.executeBatch();
                }
            }
            catch (SQLException e) {
                logger.error("close executeBatch fail", e);
            }
        }
        catch (SQLException e) {
            logger.error("close connection fail", e);
        }
    }

    public static class MysqlConfig
            extends PluginConfig
    {
        @Name("url")
        @Description("this is mysql jdbc url")
        private String jdbcUrl = "jdbc:mysql://localhost:3306/pop?characterEncoding=utf-8&useSSL=false";

        @Name("userName")
        @Description("this is mysql userName")
        private String user = "demo";

        @Name("password")
        @Description("this is mysql password")
        private String password = "demo";

        @Name("query")
        @Description("this is mysql save query")
        private String query = null;
        /*
         * demo: insert into your_table values(${0},${1},${2})
         * demo: replace into table select '${0}', ifnull((select cnt from table where id = '${0}'),0)+{1};
         * */

        public String getJdbcUrl()
        {
            return jdbcUrl;
        }

        public String getUser()
        {
            return user;
        }

        public String getPassword()
        {
            return password;
        }

        public String getQuery()
        {
            return query;
        }
    }

    private static boolean isNumeric(String str)
    {
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
