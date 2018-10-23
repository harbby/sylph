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
package ideal.sylph.runner.flink.sql;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import ideal.common.base.JdbcUtils;
import ideal.sylph.parser.calcite.JoinInfo;
import org.apache.calcite.sql.JoinType;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.calcite.sql.JoinType.LEFT;

/**
 * 这个例子研究 AsyncFunction机制
 */
public class MysqlAsyncFunction
        extends RichAsyncFunction<Row, Row>
        implements Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlAsyncFunction.class);

    private final List<Field> joinSelectFields;
    private final Map<Integer, String> joinOnMapping;
    private final String sql;
    private final JoinType joinType;
    private final int selectFieldCnt;

    private Connection connection;
    private PreparedStatement statement;

    private final Cache<String, List<Map<String, Object>>> cache = CacheBuilder.newBuilder()
            .maximumSize(1000)   //max cache 1000 value
            .expireAfterAccess(300, TimeUnit.SECONDS)  //5 minutes
            .build();

    public MysqlAsyncFunction(JoinInfo joinInfo, RowTypeInfo streamRowType, List<Field> joinSelectFields)
    {
        this.joinType = joinInfo.getJoinType();
        this.joinSelectFields = joinSelectFields;
        this.selectFieldCnt = joinSelectFields.size();
        this.joinOnMapping = joinInfo.getJoinOnMapping()
                .entrySet().stream()
                .collect(Collectors.toMap(k -> {
                    int streamFieldIndex = streamRowType.getFieldIndex(k.getKey());
                    checkState(streamFieldIndex != -1, "can't deal equal field: " + k.getKey());
                    return streamFieldIndex;
                }, Map.Entry::getValue));

        String where = joinOnMapping.values().stream().map(x -> x + " = ?").collect(Collectors.joining(" and "));
        String batchSelectField = joinSelectFields.stream().filter(Field::isBatchTableField)
                .map(Field::getFieldName).collect(Collectors.joining(","));
        String select = "select %s from %s where %s";

        if (batchSelectField.length() == 0) {
            // 没有选中任何字段
            batchSelectField = "true";
        }

        this.sql = String.format(select, batchSelectField, joinInfo.getBatchTable().getAliasOrElseName().toLowerCase(), where);
        logger.info("batch table join query is [{}]", sql);
        logger.info("join mapping is {}", joinOnMapping);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> asyncCollector)
            throws Exception
    {
        CompletableFuture<Collection<Row>> resultFuture = CompletableFuture.supplyAsync(() -> {
            try {
                checkState(connection != null, " connection is null");
                StringBuilder builder = new StringBuilder();
                for (int index : joinOnMapping.keySet()) {
                    builder.append(input.getField(index)).append("\u0001");
                }
                List<Map<String, Object>> cacheData = cache.get(builder.toString(), () -> {
                    //-- 这里进行真正的数据库查询
                    List<Integer> indexs = ImmutableList.copyOf(joinOnMapping.keySet());
                    for (int i = 0; i < indexs.size(); i++) {
                        statement.setObject(i + 1, input.getField(indexs.get(i)));
                    }

                    try (ResultSet rs = statement.executeQuery()) {
                        List<Map<String, Object>> result = JdbcUtils.resultToList(rs);
                        if (result.isEmpty() && joinType == LEFT) { // left join and inter join
                            return ImmutableList.of(ImmutableMap.of());
                        }
                        return result;
                    }
                    catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

                List<Row> rows = new ArrayList<>(cacheData.size());
                for (Map<String, Object> map : cacheData) {
                    Row row = new Row(selectFieldCnt);
                    for (int i = 0; i < selectFieldCnt; i++) {
                        Field field = joinSelectFields.get(i);
                        if (field.isBatchTableField()) {
                            row.setField(i, map.get(field.getFieldName()));
                        }
                        else {
                            row.setField(i, input.getField(field.getFieldIndex()));
                        }
                    }
                    rows.add(row);
                }
                return rows;
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        // 设置请求完成时的回调: 将结果传递给 collector
        resultFuture.whenComplete((result, error) -> {
            if (error != null) {
                //todo: 这里可以加入开关 如果关联失败是否进行置空,默认情况 整个任务会直接结束
                asyncCollector.completeExceptionally(error);
            }
            else {
                //因为一条数据 可能join出来多条 所以结果是集合
                Row row = Row.of("uid", "topic", "uid", 123L, "batch111", "batch222");
                asyncCollector.complete(result);
            }
        });
    }

    @Override
    public void open(Configuration parameters)
            throws Exception
    {
        super.open(parameters);
        //create connection
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/pop?characterEncoding=utf-8&useSSL=false";
            this.connection = DriverManager.getConnection(url, "demo", "demo");
            this.statement = connection.prepareStatement(sql);
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new SQLException("Mysql connection open fail", e);
        }
    }

    @Override
    public void close()
            throws Exception
    {
        super.close();
        try (Connection conn = connection) {
            if (statement != null) {
                statement.close();
            }
        }
    }
}
