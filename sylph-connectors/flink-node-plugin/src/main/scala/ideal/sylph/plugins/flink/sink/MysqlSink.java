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
package ideal.sylph.plugins.flink.sink;

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

    public MysqlSink(MysqlConfig mysqlConfig)
    {
        this.config = mysqlConfig;
    }

    @Override
    public boolean open(long partitionId, long version)
    {
        String sql = "insert into mysql_table_sink values(?,?,?)";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.connection = DriverManager.getConnection(config.jdbcUrl, config.user, config.password);
            this.statement = connection.prepareStatement(sql);
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("MysqlSink open fail", e);
        }
        return true;
    }

    @Override
    public void process(Row value)
    {
        try {
            for (int i = 0; i < value.size(); i++) {
                statement.setObject(i + 1, value.getAs(i));
            }
            statement.addBatch();
            // submit batch
            if (num >= 5) {
                statement.executeBatch();
                num = 0;
            }
            else {
                num++;
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
        private final String jdbcUrl;

        @Name("userName")
        @Description("this is mysql userName")
        private final String user;

        @Name("password")
        @Description("this is mysql password")
        private final String password;

        private MysqlConfig(String jdbcUrl, String user, String password)
        {
            this.jdbcUrl = jdbcUrl;
            this.user = user;
            this.password = password;
        }
    }
}
