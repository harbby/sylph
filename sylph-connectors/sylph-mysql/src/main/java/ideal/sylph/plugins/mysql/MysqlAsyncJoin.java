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
import ideal.sylph.etl.Collector;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.etl.join.JoinContext;
import ideal.sylph.etl.join.SelectField;
import ideal.sylph.plugins.mysql.utils.JdbcUtils;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static ideal.sylph.etl.join.JoinContext.JoinType.LEFT;
import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkState;

/**
 * 这个例子研究 AsyncFunction机制
 */
@Name("mysql")
@Description("this is `join mode` mysql config table")
public class MysqlAsyncJoin
        implements RealTimeTransForm
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlAsyncJoin.class);

    private final List<SelectField> selectFields;
    private final Map<Integer, String> joinOnMapping;
    private final String sql;
    private final JoinContext.JoinType joinType;
    private final int selectFieldCnt;
    private final MysqlJoinConfig config;
    private final Row.Schema schema;

    private Connection connection;

    private Cache<String, List<Map<String, Object>>> cache;

    public MysqlAsyncJoin(JoinContext context, MysqlJoinConfig mysqlConfig)
            throws Exception
    {
        this.config = mysqlConfig;
        this.schema = context.getSchema();
        this.selectFields = context.getSelectFields();
        this.selectFieldCnt = selectFields.size();
        this.joinType = context.getJoinType();
        this.joinOnMapping = context.getJoinOnMapping();

        String where = context.getJoinOnMapping().values().stream().map(x -> x + " = ?").collect(Collectors.joining(" and "));
        List<String> batchFields = context.getSelectFields().stream().filter(SelectField::isBatchTableField)
                .map(SelectField::getFieldName).collect(Collectors.toList());

        String select = "select %s from %s where %s";

        String batchSelectFields = batchFields.isEmpty() ? "true" : String.join(",", batchFields);

        String jdbcTable = config.getQuery() != null && config.getQuery().trim().length() > 0
                ? "(" + config.getQuery() + ") as " + context.getBatchTable()
                : context.getBatchTable();

        this.sql = String.format(select, batchSelectFields, jdbcTable, where);

        checkMysql(mysqlConfig, jdbcTable, ImmutableSet.<String>builder().addAll(batchFields).addAll(context.getJoinOnMapping().values()).build());

        logger.info("batch table join query is [{}]", sql);
        logger.info("join mapping is {}", context.getJoinOnMapping());

        this.cache = CacheBuilder.newBuilder()
                .maximumSize(mysqlConfig.getCacheMaxNumber())   //max cache 1000 value
                .expireAfterAccess(mysqlConfig.getCacheTime(), TimeUnit.SECONDS)  //
                .build();
    }

    private static void checkMysql(MysqlJoinConfig config, String tableName, Set<String> fieldNames)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(config.getJdbcUrl(), config.getUser(), config.getPassword());
                ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, null);
        ) {
            List<Map<String, Object>> tableSchema = JdbcUtils.resultToList(resultSet);
            List<String> listNames = tableSchema.stream().map(x -> (String) x.get("COLUMN_NAME")).collect(Collectors.toList());

            checkState(listNames.containsAll(fieldNames), "mysql table `" + tableName + " fields ` only " + listNames + ", but your is " + fieldNames);
        }
    }

    @Override
    public void process(Row input, Collector<Row> collector)
    {
        try {
            checkState(connection != null, " connection is null");
            StringBuilder builder = new StringBuilder();
            for (int index : joinOnMapping.keySet()) {
                builder.append(input.<Object>getField(index)).append("\u0001");
            }
            List<Map<String, Object>> cacheData = cache.get(builder.toString(), () -> {
                //-- 这里进行真正的数据库查询
                List<Integer> indexs = ImmutableList.copyOf(joinOnMapping.keySet());
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int i = 0; i < indexs.size(); i++) {
                        statement.setObject(i + 1, input.getField(indexs.get(i)));
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Thread is  {}, this {}", Thread.currentThread().getId(), this);
                    }
                    try (ResultSet rs = statement.executeQuery()) {
                        List<Map<String, Object>> result = JdbcUtils.resultToList(rs);
                        if (result.isEmpty() && joinType == LEFT) { // left join and inter join
                            return ImmutableList.of(ImmutableMap.of());
                        }
                        return result;
                    }
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            for (Map<String, Object> map : cacheData) {
                Object[] row = new Object[selectFieldCnt];
                for (int i = 0; i < selectFieldCnt; i++) {
                    SelectField field = selectFields.get(i);
                    if (field.isBatchTableField()) {
                        row[i] = map.get(field.getFieldName());
                    }
                    else {
                        row[i] = input.getField(field.getFieldIndex());
                    }
                }
                collector.collect(Row.of(row));
            }
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row.Schema getSchema()
    {
        return schema;
    }

    @Override
    public boolean open(long partitionId, long version)
            throws Exception
    {
        //create connection
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.connection = DriverManager.getConnection(config.getJdbcUrl(), config.getUser(), config.getPassword());
            return true;
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new SQLException("Mysql connection open fail", e);
        }
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        try (Connection conn = connection) {
            conn.isClosed();
            cache.invalidateAll();
        }
        catch (Exception e) {
        }

        if (errorOrNull != null) {
            logger.error("", errorOrNull);
        }
    }

    public static final class MysqlJoinConfig
            extends PluginConfig
    {
        @Name("cache.max.number")
        @Description("this is max cache number")
        private int maxNumber = 1000;

        @Name("cache.expire.number")
        @Description("this is cache expire SECONDS")
        private int cacheTime = 300;   // 5 minutes

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

        public int getCacheTime()
        {
            return cacheTime;
        }

        public int getCacheMaxNumber()
        {
            return maxNumber;
        }

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
