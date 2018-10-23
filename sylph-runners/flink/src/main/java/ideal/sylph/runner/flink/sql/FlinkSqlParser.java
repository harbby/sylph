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

import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.parser.calcite.CalciteSqlParser;
import ideal.sylph.parser.calcite.JoinInfo;
import ideal.sylph.parser.calcite.TableName;
import ideal.sylph.runner.flink.actuator.StreamSqlUtil;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.SqlKind.AS;
import static org.apache.calcite.sql.SqlKind.IDENTIFIER;
import static org.apache.calcite.sql.SqlKind.INSERT;
import static org.apache.calcite.sql.SqlKind.SELECT;
import static org.apache.calcite.sql.SqlKind.WITH_ITEM;

public class FlinkSqlParser
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlParser.class);

    private FlinkSqlParser() {}

    /**
     * Fully consistent with the flink sql syntax
     * <p>
     * Returns the SQL parser config for this environment including a custom Calcite configuration.
     * see {@link org.apache.flink.table.api.TableEnvironment#getSqlParserConfig}
     * <p>
     * we use Java lex because back ticks are easier than double quotes in programming
     * and cases are preserved
     */
    private static final SqlParser.Config sqlParserConfig = SqlParser
            .configBuilder()
            .setLex(Lex.JAVA)
            .build();

    public static void parser(StreamTableEnvironment tableEnv, String query, List<CreateTable> batchTablesList)
    {
        Map<String, RowTypeInfo> batchTables = batchTablesList.stream()
                .collect(Collectors.toMap(CreateTable::getName, StreamSqlUtil::getTableRowTypeInfo));
        CalciteSqlParser sqlParser = new CalciteSqlParser(batchTables.keySet());
        List<Object> execNodes = null;
        try {
            execNodes = sqlParser.parser(query, sqlParserConfig);
        }
        catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
        buildDag(tableEnv, execNodes, batchTables);
    }

    private static void buildDag(StreamTableEnvironment tableEnv, List<Object> execNodes, Map<String, RowTypeInfo> batchTables)
    {
        for (Object it : execNodes) {
            if (it instanceof SqlNode) {
                SqlNode sqlNode = (SqlNode) it;
                SqlKind sqlKind = sqlNode.getKind();
                if (sqlKind == INSERT) {
                    tableEnv.sqlUpdate(sqlNode.toString());
                }
                else if (sqlKind == AS) {
                    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                    SqlSelect sqlSelect = sqlBasicCall.operand(0);
                    String tableAlias = sqlBasicCall.operand(1).toString();
                    Table table = tableEnv.sqlQuery(sqlSelect.toString());
                    tableEnv.registerTable(tableAlias, table);
                }
                else if (sqlKind == SELECT) {
                    logger.warn("You entered the select query statement, only one for testing");
                    String sql = sqlNode.toString();
                    Table table = tableEnv.sqlQuery(sql);
                    try {
                        tableEnv.toAppendStream(table, Row.class).print();
                    }
                    catch (TableException e) {
                        tableEnv.toRetractStream(table, Row.class).print();
                    }
                }
                else if (sqlKind == WITH_ITEM) {
                    SqlWithItem sqlWithItem = (SqlWithItem) sqlNode;
                    String tableAlias = sqlWithItem.name.toString();
                    Table table = tableEnv.sqlQuery(sqlWithItem.query.toString());
                    tableEnv.registerTable(tableAlias, table);
                }
            }
            else if (it instanceof JoinInfo) {
                JoinInfo joinInfo = (JoinInfo) it;

                Table streamTable = getTable(tableEnv, joinInfo.getStreamTable());
                RowTypeInfo streamTableRowType = new RowTypeInfo(streamTable.getSchema().getTypes(), streamTable.getSchema().getColumnNames());

                RowTypeInfo batchTableRowType = requireNonNull(batchTables.get(joinInfo.getBatchTable().getName()), "batch table [" + joinInfo.getJoinTableName() + "] not exits");
                List<Field> joinSelectFields = getAllSelectFields(joinInfo, streamTableRowType, batchTableRowType);

                DataStream<Row> inputStream = tableEnv.toAppendStream(streamTable, org.apache.flink.types.Row.class);

                //It is recommended to do keyby first.
                DataStream<Row> joinResultStream = AsyncDataStream.orderedWait(
                        inputStream, new MysqlAsyncFunction(joinInfo, streamTableRowType, joinSelectFields),
                        1000, TimeUnit.MILLISECONDS, // 超时时间
                        100);  // 进行中的异步请求的最大数量

                List<String> fieldNames = new ArrayList<>();
                joinSelectFields.stream().map(Field::getFieldName).forEach(name -> {
                    String newName = name;
                    for (int i = 0; fieldNames.contains(newName); i++) {
                        newName = name + i;
                    }
                    fieldNames.add(newName);
                });
                TypeInformation[] fieldTypes = joinSelectFields.stream().map(Field::getType).toArray(TypeInformation[]::new);
                RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames.toArray(new String[0]));

                joinResultStream.getTransformation().setOutputType(rowTypeInfo);
                tableEnv.registerDataStream(joinInfo.getJoinTableName(), joinResultStream, String.join(",", fieldNames));

                //next update join select query
                joinSelectUp(joinInfo, streamTableRowType, batchTableRowType, fieldNames);
            }
        }
    }

    private static Table getTable(StreamTableEnvironment tableEnv, TableName tableName)
    {
        Table table = null;
        if (StringUtils.isNotBlank(tableName.getName())) {
            table = tableEnv.scan(tableName.getName());
        }
        else if (tableName.getAlias().isPresent()) {
            table = tableEnv.scan(tableName.getAlias().get());
        }

        checkState(table != null, "not register table " + tableName);
        return table;
    }

    private static void joinSelectUp(JoinInfo joinInfo, RowTypeInfo streamTableRowType, RowTypeInfo batchTableRowType, List<String> upFieldNames)
    {
        SqlSelect sqlSelect = joinInfo.getJoinSelect();
        String joinOutTableName = joinInfo.getJoinTableName();

        //------parser select Fields
        SqlNodeList selectNodes = new SqlNodeList(sqlSelect.getSelectList().getParserPosition());
        for (String fieldName : upFieldNames) {
            SqlNode upNode = new SqlIdentifier(fieldName, new SqlParserPos(0, 0));
            selectNodes.add(upNode);
        }
        sqlSelect.setSelectList(selectNodes);

        //--- parser having ---
        SqlNode havingNode = sqlSelect.getHaving();
        //sqlSelect.setWhere();
        if (havingNode != null) {
            SqlNode[] whereNodes = ((SqlBasicCall) havingNode).getOperands();
            for (int i = 0; i < whereNodes.length; i++) {
                SqlNode whereSqlNode = whereNodes[i];
                SqlNode upNode = updateSelectFieldName(whereSqlNode, joinOutTableName);
                whereNodes[i] = upNode;
            }
        }

        //where ...
    }

    private static SqlNode updateSelectFieldName(SqlNode inSqlNode, String joinOutTableName)
    {
        if (inSqlNode.getKind() == IDENTIFIER) {
            SqlIdentifier field = ((SqlIdentifier) inSqlNode);
            if (field.isStar()) {
                return field.setName(0, joinOutTableName);
            }
            else {
                return field.setName(0, joinOutTableName);
            }
        }
        else if (inSqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) inSqlNode;
            for (int i = 0; i < sqlBasicCall.getOperandList().size(); i++) {
                SqlNode sqlNode = sqlBasicCall.getOperandList().get(i);
                SqlNode upNode = updateSelectFieldName(sqlNode, joinOutTableName);
                sqlBasicCall.getOperands()[i] = upNode;
            }
            return sqlBasicCall;
        }
        else {
            return inSqlNode;
        }
    }

    private static List<Field> getAllSelectFields(JoinInfo joinInfo, RowTypeInfo streamTableRowType, RowTypeInfo batchTableRowType)
    {
        String streamTable = joinInfo.getStreamTable().getAliasOrElseName();
        String batchTable = joinInfo.getBatchTable().getAliasOrElseName();

        final Map<String, RowTypeInfo> tableTypes = new HashMap<>();
        tableTypes.put(streamTable, streamTableRowType);
        tableTypes.put(batchTable, batchTableRowType);

        final ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
        for (SqlNode sqlNode : joinInfo.getJoinSelect().getSelectList().getList()) {
            SqlIdentifier sqlIdentifier;
            if (sqlNode.getKind() == IDENTIFIER) {
                sqlIdentifier = (SqlIdentifier) sqlNode;
            }
            else if (sqlNode instanceof SqlBasicCall) {
                sqlIdentifier = ((SqlBasicCall) sqlNode).operand(0);
            }
            else {
                throw new IllegalArgumentException(sqlNode + "--" + sqlNode.getKind());
            }

            String tableName = sqlIdentifier.names.get(0);
            RowTypeInfo tableRowType = requireNonNull(tableTypes.get(tableName), "Unknown identifier '" + tableName + "' , with " + sqlIdentifier);
            boolean isBatchField = batchTable.equalsIgnoreCase(tableName);

            if (sqlIdentifier.isStar()) {
                for (int i = 0; i < tableRowType.getArity(); i++) {
                    Field field = Field.of(tableRowType.getFieldNames()[i], tableRowType.getFieldTypes()[i], tableName, isBatchField, i);
                    fieldBuilder.add(field);
                }
            }
            else {
                String fieldName = sqlIdentifier.names.get(1);
                int fieldIndex = tableRowType.getFieldIndex(fieldName);
                checkState(fieldIndex != -1, "table " + tableName + " not exists field:" + fieldName);
                if (sqlNode instanceof SqlBasicCall) {
                    // if(field as newName)  { use newName }
                    fieldName = ((SqlIdentifier) ((SqlBasicCall) sqlNode).operand(1)).names.get(0).toLowerCase();
                }
                fieldBuilder.add(Field.of(fieldName, tableRowType.getFieldTypes()[fieldIndex], tableName, isBatchField, fieldIndex));
            }
        }

        return fieldBuilder.build();
    }
}
