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
package ideal.sylph.plugins.jdbc;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.Collector;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.etl.join.JoinContext;
import ideal.sylph.etl.join.SelectField;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.Throwables.noCatch;
import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static ideal.sylph.etl.join.JoinContext.JoinType.LEFT;

public abstract class JdbcAsyncJoin
        implements RealTimeTransForm
{
    private static final Logger logger = LoggerFactory.getLogger(JdbcAsyncJoin.class);

    private final List<SelectField> selectFields;
    private final Map<Integer, String> joinOnMapping;
    private final String sql;
    private final JoinContext.JoinType joinType;
    private final int selectFieldCnt;
    private final JdbcJoinConfig config;
    private final Schema schema;

    private Connection connection;
    private Cache<String, List<Map<String, Object>>> cache;

    public JdbcAsyncJoin(JoinContext context, JdbcJoinConfig jdbcJoinConfig)
    {
        this.config = jdbcJoinConfig;
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

        logger.info("batch table join query is [{}]", sql);
        logger.info("join mapping is {}", context.getJoinOnMapping());

        this.cache = CacheBuilder.newBuilder()
                .maximumSize(jdbcJoinConfig.getCacheMaxNumber())   //max cache 1000 value
                .expireAfterWrite(jdbcJoinConfig.getCacheTime(), TimeUnit.SECONDS)
                //.expireAfterAccess(mysqlConfig.getCacheTime(), TimeUnit.SECONDS)  //
                .build();
    }

    @Override
    public void process(Row input, Collector<Row> collector)
    {
        checkState(connection != null, " connection is null");

        StringBuilder builder = new StringBuilder();
        for (int index : joinOnMapping.keySet()) {
            builder.append(input.<Object>getField(index)).append("\u0001");
        }

        List<Map<String, Object>> cacheData = noCatch(() -> cache.get(builder.toString(), () -> {
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
                throw throwsException(e);
            }
        }));

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

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public boolean open(long partitionId, long version)
            throws Exception
    {
        Class.forName(getJdbcDriver());
        this.connection = DriverManager.getConnection(config.getJdbcUrl(), config.getUser(), config.getPassword());
        return true;
    }

    public abstract String getJdbcDriver();

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

    public static final class JdbcJoinConfig
            extends PluginConfig
    {
        @Name("cache.max.number")
        @Description("this is max cache number")
        private long maxNumber = 1000;

        @Name("cache.expire.number")
        @Description("this is cache expire SECONDS")
        private long cacheTime = 300;   // 5 minutes

        @Name("url")
        @Description("this is jdbc url")
        private String jdbcUrl = "jdbc:mysql://localhost:3306/pop?characterEncoding=utf-8&useSSL=false";

        @Name("userName")
        @Description("this is mysql userName")
        private String user = "demo";

        @Name("password")
        @Description("this is mysql password")
        private String password = "demo";

        @Name("query")
        @Description("this is jdbc join push down query")
        private String query = null;

        public int getCacheTime()
        {
            return (int) cacheTime;
        }

        public int getCacheMaxNumber()
        {
            return (int) maxNumber;
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
