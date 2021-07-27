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
package ideal.sylph.runner.flink.engines;

import com.github.harbby.gadtry.collection.MutableList;
import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.google.common.collect.ImmutableList;
import ideal.sylph.TableContext;
import ideal.sylph.etl.Schema;
import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.ParsingException;
import ideal.sylph.parser.antlr.tree.CreateFunction;
import ideal.sylph.parser.antlr.tree.CreateStreamAsSelect;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.parser.antlr.tree.InsertInto;
import ideal.sylph.parser.antlr.tree.Proctime;
import ideal.sylph.parser.antlr.tree.SelectQuery;
import ideal.sylph.parser.antlr.tree.Statement;
import ideal.sylph.parser.antlr.tree.WaterMark;
import ideal.sylph.runner.flink.FlinkBean;
import ideal.sylph.runner.flink.etl.FlinkNodeLoader;
import ideal.sylph.runner.flink.sql.FlinkSqlParser;
import ideal.sylph.runner.flink.sql.TriggerWindowHelper;
import ideal.sylph.runner.flink.table.SylphTableSink;
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.NodeLoader;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static ideal.sylph.parser.antlr.tree.CreateTable.Type.SINK;
import static ideal.sylph.parser.antlr.tree.CreateTable.Type.SOURCE;
import static ideal.sylph.runner.flink.engines.StreamSqlUtil.assignWaterMark;
import static ideal.sylph.runner.flink.engines.StreamSqlUtil.checkStream;
import static ideal.sylph.runner.flink.engines.StreamSqlUtil.getTableSchema;
import static ideal.sylph.runner.flink.engines.StreamSqlUtil.schemaToRowTypeInfo;

public class StreamSqlBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSqlBuilder.class);

    private final ConnectorStore connectorStore;
    private final StreamTableEnvironment tableEnv;
    private final StreamExecutionEnvironment execEnv;
    private final AntlrSqlParser sqlParser;

    private final List<CreateTable> batchTables = new ArrayList<>();
    private final TriggerWindowHelper triggerHelper = new TriggerWindowHelper();

    public StreamSqlBuilder(
            StreamTableEnvironment tableEnv,
            ConnectorStore connectorStore,
            AntlrSqlParser sqlParser
    )
    {
        this.connectorStore = connectorStore;
        this.tableEnv = tableEnv;
        this.execEnv = ((StreamTableEnvironmentImpl) tableEnv).execEnv();
        this.sqlParser = sqlParser;
    }

    public void buildStreamBySql(String sql)
    {
        FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                .setTableEnv(tableEnv)
                .setConnectorStore(connectorStore)
                .build();
        Statement statement = sqlParser.createStatement(sql);

        if (statement instanceof CreateStreamAsSelect) {
            CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
            Table table = tableEnv.sqlQuery(createStreamAsSelect.getViewSql());
            RowTypeInfo rowTypeInfo = (RowTypeInfo) table.getSchema().toRowType();
            DataStream<Row> stream = tableEnv.toAppendStream(table, Row.class);
            stream.getTransformation().setOutputType(rowTypeInfo);

            registerStreamTable(stream, createStreamAsSelect.getName(), createStreamAsSelect.getWatermark(), Optional.empty());
        }
        else if (statement instanceof CreateTable) {
            if (((CreateTable) statement).getType() == CreateTable.Type.BATCH) {
                batchTables.add((CreateTable) statement);
            }
            else {
                createStreamTable((CreateTable) statement);
            }
        }
        else if (statement instanceof CreateFunction) {
            createFunction((CreateFunction) statement);
        }
        else if (statement instanceof SelectQuery) {
            SelectQuery selectQuery = (SelectQuery) statement;
            String pushDownQuery = selectQuery.getQuery();
            Table table = flinkSqlParser.parser(pushDownQuery, ImmutableList.copyOf(batchTables));
            try {
                tableEnv.toAppendStream(table, Row.class).print();
            }
            catch (ValidationException e) {
                checkState(e.getMessage().equals("Table is not an append-only table. Use the toRetractStream() in order to handle add and retract messages."),
                        "sylph and flink versions are not compatible, please feedback to the community");
                tableEnv.toRetractStream(table, Row.class).print();
            }
            //----------------------------
            //todo: java.lang.IllegalStateException: No operators defined in streaming topology. Cannot execute.
            //triggerHelper.settingTrigger(execEnv.getStreamGraph(), selectQuery);
        }
        else if (statement instanceof InsertInto) {
            InsertInto insertInto = (InsertInto) statement;
            SelectQuery selectQuery = insertInto.getSelectQuery();
            String pushDownQuery = selectQuery.getQuery();
            Table table = flinkSqlParser.parser(pushDownQuery, ImmutableList.copyOf(batchTables));
            if (table == null) {
                throw new ParsingException("table is null");
            }
            try {
                table.insertInto(insertInto.getTableName());
            }
            catch (TableException e) {
                checkState(e.getMessage().equals("AppendStreamTableSink requires that Table has only insert changes."),
                        "sylph and flink versions are not compatible, please feedback to the community");
                DataStream<Row> retractStream = tableEnv.toRetractStream(table, Row.class)
                        .filter(x -> x.f0).map(x -> x.f1).returns(table.getSchema().toRowType());
                tableEnv.fromDataStream(retractStream).insertInto(insertInto.getTableName());
            }
            //----------------------------
            //todo: java.lang.IllegalStateException: No operators defined in streaming topology. Cannot execute.
            //triggerHelper.settingTrigger(execEnv.getStreamGraph(), selectQuery);
        }
        else {
            throw new IllegalArgumentException("this driver class " + statement.getClass() + " have't support!");
        }
    }

    private void createFunction(CreateFunction createFunction)
    {
        Object function = null;
        try {
            Class driver = Class.forName(createFunction.getClassString());
            function = driver.newInstance();
        }
        catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new IllegalArgumentException("create function failed " + createFunction, e);
        }
        if (function instanceof AggregateFunction) {
            tableEnv.registerFunction(createFunction.getFunctionName(), (AggregateFunction<?, ?>) function);
        }
        else if (function instanceof TableFunction) {
            tableEnv.registerFunction(createFunction.getFunctionName(), (TableFunction<?>) function);
        }
        else if (function instanceof ScalarFunction) {
            tableEnv.registerFunction(createFunction.getFunctionName(), (ScalarFunction) function);
        }
    }

    private void createStreamTable(CreateTable createStream)
    {
        final String tableName = createStream.getName();
        Schema schema = getTableSchema(createStream);
        RowTypeInfo tableTypeInfo = schemaToRowTypeInfo(schema);

        final Map<String, Object> withConfig = createStream.getWithProperties();
        final String connector = createStream.getConnector();

        Bean bean = binder -> binder.bind(TableContext.class, new TableContext()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public String getTableName()
            {
                return tableName;
            }

            @Override
            public String getConnector()
            {
                return connector;
            }

            @Override
            public Map<String, Object> withConfig()
            {
                return withConfig;
            }
        });
        final IocFactory iocFactory = IocFactory.create(new FlinkBean(execEnv, tableEnv), bean);
        NodeLoader<DataStream<Row>> loader = new FlinkNodeLoader(connectorStore, iocFactory);

        if (SOURCE == createStream.getType()) {
            DataStream<Row> inputStream = checkStream(loader.loadSource(connector, withConfig).apply(null), tableTypeInfo);
            //---------------------------------------------------
            registerStreamTable(inputStream, tableName, createStream.getWatermark(), createStream.getProctimes());
        }
        else if (SINK == createStream.getType()) {
            UnaryOperator<DataStream<Row>> outputStream = loader.loadSink(connector, withConfig);
            SylphTableSink tableSink = new SylphTableSink(tableTypeInfo, outputStream);
            ((TableEnvironmentInternal) tableEnv).registerTableSinkInternal(tableName, tableSink);
        }
        else {
            throw new IllegalArgumentException("this Connector " + createStream.getConnector() + " haven't support!");
        }
    }

    private void registerStreamTable(
            DataStream<Row> inputStream,
            String tableName,
            Optional<WaterMark> waterMarkOptional,
            Optional<Proctime> proctimes)
    {
        RowTypeInfo tableTypeInfo = (RowTypeInfo) inputStream.getType();
        List<String> fields = MutableList.of(tableTypeInfo.getFieldNames());
        proctimes.map(x -> x.getName() + ".proctime").ifPresent(fields::add);

        if (waterMarkOptional.isPresent()) {
            logger.info("createStreamTable Watermark is {}", waterMarkOptional.get());
            /*
             * Deprecated
             * In Flink 1.12 the default stream time characteristic has been changed to TimeCharacteristic.EventTime,
             * thus you don't need to call this method for enabling event-time support anymore.
             *
             * execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
             */
            if (proctimes.isPresent() && waterMarkOptional.get().getFieldName().equals(proctimes.get().getName())) {
                throw new ParsingException("waterMark don't support proctime()");
            }
            DataStream<Row> waterMarkStream = assignWaterMark(waterMarkOptional.get(), tableTypeInfo, inputStream);
            fields.add(waterMarkOptional.get().getRowTimeName() + ".rowtime");
            tableEnv.registerDataStream(tableName, waterMarkStream, String.join(",", fields));
        }
        else {
            tableEnv.registerDataStream(tableName, inputStream, String.join(",", fields));
        }
    }
}
