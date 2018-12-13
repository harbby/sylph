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
package ideal.sylph.plugins.clickhouse;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.api.RealTimeSink;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkState;


@Name("ClickHouseSink")
@Description("this is ClickHouseSink sink plugin")
public class ClickHouseSink
        implements RealTimeSink
{
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSink.class);

    private final ClickHouseSinkConfig config;
    private final String prepareStatementQuery;
    private final String[] keys;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private int num = 0;

    public ClickHouseSink(ClickHouseSinkConfig clickHouseSinkConfig)
    {
        this.config = clickHouseSinkConfig;
        checkState(config.getQuery() != null, "insert into query not setting");
        this.prepareStatementQuery = config.getQuery().replaceAll("\\$\\{.*?}", "?");
        // parser sql query ${key}
        Matcher matcher = Pattern.compile("(?<=\\$\\{)(.+?)(?=\\})").matcher(config.getQuery());
        List<String> builder = new ArrayList<>();
        while (matcher.find()) {
            builder.add(matcher.group());
        }
        this.keys = builder.toArray(new String[0]);
    }

    @Override
    public void process(Row row){

        // type convert

//        case "DateTime" | "Date" | "String" => statement.setString(i + 1, item.getAs[String](field))
//        case "Int8" | "Int16" | "Int32" | "UInt8" | "UInt16" => statement.setInt(i + 1, item.getAs[Int](field))
//        case "UInt64" | "Int64" | "UInt32" => statement.setLong(i + 1, item.getAs[Long](field))
//        case "Float32" | "Float64" => statement.setDouble(i + 1, item.getAs[Double](field))
//        case _ => statement.setString(i + 1, item.getAs[String](field))


//         pstmt.setString(1, lines[1]);
//         pstmt.setString(2, lines[3]);
//         pstmt.setString(3, lines[4]);
//         pstmt.setString(4, lines[6]);
//         pstmt.addBatch();


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
    public boolean open(long partitionId, long version) throws SQLException, ClassNotFoundException
    {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        this.connection = DriverManager.getConnection(config.jdbcUrl, config.user, config.password);
        this.statement = connection.prepareStatement(prepareStatementQuery);
        return true;
    }

    @Override
    public void close(Throwable errorOrNull){

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

    public static class ClickHouseSinkConfig extends PluginConfig{

        @Name("url")
        @Description("this is ck jdbc url")
        private String jdbcUrl = "jdbc:clickhouse://localhost:9000";

        @Name("userName")
        @Description("this is ck userName")
        private String user = "default";

        @Name("password")
        @Description("this is ck password")
        private String password = "default";

        @Name("query")
        @Description("this is ck save query")
        private String query = null;

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }

        public String getQuery() {
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
