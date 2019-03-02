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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkState;

@Name("ClickHouseSink")
@Description("this is ClickHouseSink sink plugin")
public class ClickHouseSink
        implements RealTimeSink
{
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSink.class);

    private final ClickHouseSinkConfig config;
    private final String prepareStatementQuery;
    private final Row.Schema schema;
    private int idIndex = -1;
    private transient Connection connection;
    private transient PreparedStatement statement;
    private int num = 0;
    private final Map<String, String> nametypes;

    public ClickHouseSink(SinkContext context, ClickHouseSinkConfig clickHouseSinkConfig)
    {
        this.config = clickHouseSinkConfig;
        checkState(config.getQuery() != null, "insert into query not setting");
        this.prepareStatementQuery = config.getQuery().replaceAll("\\$\\{.*?}", "?");
        schema = context.getSchema();
        Map<String, String> nt = new HashMap<String, String>();
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            nt.put(schema.getFieldNames().get(i), schema.getFieldTypes().get(i).toString().split(" ")[1]);
        }
        this.nametypes = nt;
    }

    @Override
    public void process(Row row)
    {

        try {
            int ith = 1;
            for (String fieldName : schema.getFieldNames()) {
                //Byte  Double  String  Date  Long  .....
                if (nametypes.get(fieldName).equals("java.sql.Date")) {
                    statement.setDate(ith, java.sql.Date.valueOf(row.getAs(fieldName).toString()));
                }
                else if ((nametypes.get(fieldName).equals("java.lang.Long"))) {
                    statement.setLong(ith, row.getAs(fieldName));
                }
                else if ((nametypes.get(fieldName).equals("java.lang.Double"))) {
                    statement.setDouble(ith, row.getAs(fieldName));
                }
                else if ((nametypes.get(fieldName).equals("java.lang.Integer"))) {
                    statement.setByte(ith, Byte.valueOf(row.getAs(fieldName)));
                }
                else {
                    statement.setString(ith, row.getAs(fieldName));
                }
                ith += 1;
            }
            statement.addBatch();
            if (num++ >= config.bulkSize) {
                statement.executeBatch();
                num = 0;
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean open(long partitionId, long version)
            throws SQLException, ClassNotFoundException
    {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        this.connection = DriverManager.getConnection(config.jdbcUrl, config.user, config.password);
        this.statement = connection.prepareStatement(prepareStatementQuery);
        return true;
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

    public static class ClickHouseSinkConfig
            extends PluginConfig
    {
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

        @Name("bulkSize")
        @Description("this is ck bulkSize")
        private int bulkSize = 20000;

        @Name("eventDate_field")
        @Description("this is your data eventDate_field, 必须是 YYYY-mm--dd位时间戳")
        private String eventTimeName;

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
}
