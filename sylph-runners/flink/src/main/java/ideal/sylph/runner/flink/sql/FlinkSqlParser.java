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

import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.etl.Operator;
import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.etl.join.JoinContext;
import ideal.sylph.etl.join.SelectField;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.parser.calcite.CalciteSqlParser;
import ideal.sylph.parser.calcite.JoinInfo;
import ideal.sylph.parser.calcite.TableName;
import ideal.sylph.runner.flink.engines.StreamSqlUtil;
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.NodeLoader;
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
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
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
     * see {@link org.apache.flink.table.planner.PlanningConfigurationBuilder#getSqlParserConfig}
     * <p>
     * we use Java lex because back ticks are easier than double quotes in programming
     * and cases are preserved
     */
    private SqlParser.Config sqlParserConfig = SqlParser
            .configBuilder()
            .setLex(Lex.JAVA)
            .build();

    private StreamTableEnvironment tableEnv;
    private ConnectorStore connectorStore;

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Builder() {}

        private final FlinkSqlParser sqlParser = new FlinkSqlParser();

        public Builder setTableEnv(StreamTableEnvironment tableEnv)
        {
            sqlParser.tableEnv = tableEnv;
            return Builder.this;
        }

        public Builder setConnectorStore(ConnectorStore connectorStore)
        {
            sqlParser.connectorStore = connectorStore;
            return Builder.this;
        }

        public Builder setParserConfig(SqlParser.Config sqlParserConfig)
        {
            sqlParser.sqlParserConfig = sqlParserConfig;
            return Builder.this;
        }

        public FlinkSqlParser build()
        {
            checkState(sqlParser.sqlParserConfig != null);
            checkState(sqlParser.tableEnv != null);
            checkState(sqlParser.connectorStore != null);
            return sqlParser;
        }
    }

    public Table parser(String query, List<CreateTable> batchTablesList)
    {
        Map<String, CreateTable> batchTables = batchTablesList.stream()
                .collect(Collectors.toMap(CreateTable::getName, v -> v));
        CalciteSqlParser sqlParser = new CalciteSqlParser(batchTables.keySet());

        List<Object> plan;
        try {
            plan = sqlParser.getPlan(query, sqlParserConfig);
        }
        catch (SqlParseException e) {
            throw throwsException(e);
        }

        List<String> registerViews = new ArrayList<>();
        try {
            return translate(plan, batchTables, registerViews);
        }
        finally {
            //registerViews.forEach(tableName -> tableEnv.sqlQuery("drop table " + tableName));
        }
    }

    private Table translate(List<Object> execNodes, Map<String, CreateTable> batchTables, List<String> registerViews)
    {
        for (Object it : execNodes) {
            if (it instanceof SqlNode) {
                SqlNode sqlNode = (SqlNode) it;
                SqlKind sqlKind = sqlNode.getKind();
                if (sqlKind == INSERT) {
                    tableEnv.sqlUpdate(sqlNode.toString());
                }
                else if (sqlKind == AS) {  //Subquery
                    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                    SqlSelect sqlSelect = sqlBasicCall.operand(0);
                    String tableAlias = sqlBasicCall.operand(1).toString();
                    Table table = tableEnv.sqlQuery(sqlSelect.toString());
                    tableEnv.registerTable(tableAlias, table);
                    registerViews.add(tableAlias);
                }
                else if (sqlKind == SELECT) {
                    logger.warn("You entered the select query statement, only one for testing");
                    String sql = sqlNode.toString();
                    return tableEnv.sqlQuery(sql);
                }
                else if (sqlKind == WITH_ITEM) {
                    SqlWithItem sqlWithItem = (SqlWithItem) sqlNode;
                    String tableAlias = sqlWithItem.name.toString();
                    Table table = tableEnv.sqlQuery(sqlWithItem.query.toString());
                    tableEnv.registerTable(tableAlias, table);
                    registerViews.add(tableAlias);
                }
            }
            else if (it instanceof JoinInfo) {
                translateJoin((JoinInfo) it, batchTables);
            }
            else {
                throw new IllegalArgumentException(it.toString());
            }
        }
        return null;
    }

    private void translateJoin(JoinInfo joinInfo, Map<String, CreateTable> batchTables)
    {
        Table streamTable = getTable(tableEnv, joinInfo.getStreamTable());
        RowTypeInfo streamRowType = (RowTypeInfo) streamTable.getSchema().toRowType();
        DataStream<Row> inputStream = tableEnv.toAppendStream(streamTable, org.apache.flink.types.Row.class);
        inputStream.getTransformation().setOutputType(streamRowType);

        //get batch table schema
        CreateTable batchTable = requireNonNull(batchTables.get(joinInfo.getBatchTable().getName()), "batch table [" + joinInfo.getJoinTableName() + "] not exits");
        RowTypeInfo batchTableRowType = StreamSqlUtil.schemaToRowTypeInfo(StreamSqlUtil.getTableSchema(batchTable));
        List<SelectField> joinSelectFields = getAllSelectFields(joinInfo, streamRowType, batchTableRowType);

        //It is recommended to do keyby first.
        JoinContext joinContext = JoinContextImpl.createContext(joinInfo, streamRowType, joinSelectFields);
        RealTimeTransForm transForm = getJoinTransForm(joinContext, batchTable);
        DataStream<Row> joinResultStream = AsyncFunctionHelper.translate(inputStream, transForm);

        //set schema
        RowTypeInfo rowTypeInfo = getJoinOutScheam(joinSelectFields);
        joinResultStream.getTransformation().setOutputType(rowTypeInfo);
        //--register tmp joinTable

        Catalog catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get();
        if (catalog.tableExists(ObjectPath.fromString(joinInfo.getJoinTableName()))) {
            Table table = tableEnv.fromDataStream(joinResultStream);
            CatalogBaseTable tableTable = new QueryOperationCatalogView(table.getQueryOperation());
            try {
                catalog.createTable(ObjectPath.fromString(joinInfo.getJoinTableName()), tableTable, true);
            }
            catch (TableAlreadyExistException | DatabaseNotExistException e) {
                e.printStackTrace();
            }
            //tableEnv.replaceRegisteredTable(joinInfo.getJoinTableName(), new RelTable(table.getRelNode()));
        }
        else {
            tableEnv.registerDataStream(joinInfo.getJoinTableName(), joinResultStream);
        }
        //next update join select query
        joinQueryUpdate(joinInfo, rowTypeInfo.getFieldNames());
    }

    private RealTimeTransForm getJoinTransForm(JoinContext joinContext, CreateTable batchTable)
    {
        Map<String, Object> withConfig = batchTable.getWithConfig();
        String driverOrName = (String) withConfig.get("type");
        Class<?> driver = connectorStore.getConnectorDriver(driverOrName, Operator.PipelineType.transform);
        checkState(RealTimeTransForm.class.isAssignableFrom(driver), "batch table type driver must is RealTimeTransForm");

        // instance
        IocFactory iocFactory = IocFactory.create(binder -> binder.bind(JoinContext.class, joinContext));

        return NodeLoader.getPluginInstance(driver.asSubclass(RealTimeTransForm.class), iocFactory, ImmutableMap.copyOf(withConfig));
    }

    private static RowTypeInfo getJoinOutScheam(List<SelectField> joinSelectFields)
    {
        List<String> fieldNames = new ArrayList<>();
        joinSelectFields.stream().map(SelectField::getFieldName).forEach(name -> {
            String newName = name;
            for (int i = 0; fieldNames.contains(newName); i++) {
                newName = name + i;
            }
            fieldNames.add(newName);
        });
        TypeInformation[] fieldTypes = joinSelectFields.stream()
                .map(SelectField::getType)
                .map(TypeInformation::of)
                .toArray(TypeInformation[]::new);

        return new RowTypeInfo(fieldTypes, fieldNames.toArray(new String[0]));
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

    private static void joinQueryUpdate(JoinInfo joinInfo, String[] upFieldNames)
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

        //parser where
        // Pushdown across data sources is not supported at this time
        if (sqlSelect.getWhere() != null) {
            throw new UnsupportedOperationException("stream join batch Where filtering is not supported, Please consider using `having`;\n" +
                    " will ignore where " + sqlSelect.getWhere().toString());
        }

        //--- parser having ---
        SqlNode havingNode = sqlSelect.getHaving();
        if (havingNode != null) {
            SqlNode[] filterNodes = ((SqlBasicCall) havingNode).getOperands();
            for (int i = 0; i < filterNodes.length; i++) {
                SqlNode whereSqlNode = filterNodes[i];
                SqlNode upNode = updateOnlyOneFilter(whereSqlNode, joinOutTableName);
                filterNodes[i] = upNode;
            }

            sqlSelect.setHaving(null);
            sqlSelect.setWhere(havingNode);
        }
    }

    /**
     * update having
     */
    private static SqlNode updateOnlyOneFilter(SqlNode filterNode, String joinOutTableName)
    {
        if (filterNode.getKind() == IDENTIFIER) {
            SqlIdentifier field = ((SqlIdentifier) filterNode);
            checkState(!field.isStar(), "filter field must not Star(*)");
            if (field.names.size() > 1) {
                field.setName(0, field.getComponent(0).getSimple());
                field.setName(1, joinOutTableName);
            }
            return field;
        }
        else if (filterNode instanceof SqlBasicCall) {  //demo: `user_id` = 'uid_1'
            SqlBasicCall sqlBasicCall = (SqlBasicCall) filterNode;
            for (int i = 0; i < sqlBasicCall.getOperandList().size(); i++) {
                SqlNode sqlNode = sqlBasicCall.getOperandList().get(i);
                SqlNode upNode = updateOnlyOneFilter(sqlNode, joinOutTableName);
                sqlBasicCall.getOperands()[i] = upNode;
            }
            return sqlBasicCall;
        }
        else {
            return filterNode;
        }
    }

    private static List<SelectField> getAllSelectFields(JoinInfo joinInfo, RowTypeInfo streamTableRowType, RowTypeInfo batchTableRowType)
    {
        String streamTable = joinInfo.getStreamTable().getAliasOrElseName();
        String batchTable = joinInfo.getBatchTable().getAliasOrElseName();

        final Map<String, RowTypeInfo> tableTypes = new HashMap<>();
        tableTypes.put(streamTable, streamTableRowType);
        tableTypes.put(batchTable, batchTableRowType);

        final ImmutableList.Builder<SelectField> fieldBuilder = ImmutableList.builder();
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
                    SelectField field = SelectField.of(tableRowType.getFieldNames()[i], tableRowType.getFieldTypes()[i].getTypeClass(), tableName, isBatchField, i);
                    fieldBuilder.add(field);
                }
            }
            else {
                String fieldName = sqlIdentifier.names.get(1);
                int fieldIndex = tableRowType.getFieldIndex(fieldName);
                checkState(fieldIndex != -1, "table " + tableName + " not exists field:" + fieldName);
                if (sqlNode instanceof SqlBasicCall) {
                    // if(field as newName)  { use newName }
                    fieldName = ((SqlIdentifier) ((SqlBasicCall) sqlNode).operand(1)).names.get(0);
                }
                fieldBuilder.add(SelectField.of(fieldName, tableRowType.getFieldTypes()[fieldIndex].getTypeClass(), tableName, isBatchField, fieldIndex));
            }
        }

        return fieldBuilder.build();
    }
}
