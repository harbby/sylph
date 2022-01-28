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
package com.github.harbby.sylph.runner.flink.engines;

import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.sylph.api.RealTimeSink;
import com.github.harbby.sylph.api.Schema;
import com.github.harbby.sylph.api.Sink;
import com.github.harbby.sylph.api.Source;
import com.github.harbby.sylph.api.TableContext;
import com.github.harbby.sylph.parser.tree.CreateFunction;
import com.github.harbby.sylph.parser.tree.CreateStreamAsSelect;
import com.github.harbby.sylph.parser.tree.CreateTable;
import com.github.harbby.sylph.parser.tree.InsertInto;
import com.github.harbby.sylph.parser.tree.Proctime;
import com.github.harbby.sylph.parser.tree.SelectQuery;
import com.github.harbby.sylph.parser.tree.Statement;
import com.github.harbby.sylph.parser.tree.WaterMark;
import com.github.harbby.sylph.runner.flink.FlinkBean;
import com.github.harbby.sylph.runner.flink.FlinkOperatorFactory;
import com.github.harbby.sylph.runner.flink.runtime.FlinkSink;
import com.github.harbby.sylph.spi.job.SqlJobParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.sylph.parser.tree.CreateTable.Type.SINK;
import static com.github.harbby.sylph.parser.tree.CreateTable.Type.SOURCE;
import static com.github.harbby.sylph.runner.flink.engines.StreamSqlUtil.getTableSchema;

public class StreamSqlBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSqlBuilder.class);

    private final StreamTableEnvironment tableEnv;
    private final StreamExecutionEnvironment execEnv;
    private final Map<String, Sink<DataStream<Row>>> sinkTables = new HashMap<>();

    public StreamSqlBuilder(StreamTableEnvironment tableEnv)
    {
        this.tableEnv = tableEnv;
        this.execEnv = ((StreamTableEnvironmentImpl) tableEnv).execEnv();
    }

    public void buildStreamBySql(SqlJobParser.StatementNode statementNode)
    {
        Statement statement = statementNode.getStatement();
        if (statement instanceof CreateStreamAsSelect) {
            CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
            Table table = tableEnv.sqlQuery(createStreamAsSelect.getViewSql());
            DataStream<Row> stream = tableEnv.toDataStream(table, Row.class);
            registerStreamTable(stream, createStreamAsSelect.getName(), createStreamAsSelect.getWatermark(), Optional.empty());
        }
        else if (statement instanceof CreateTable) {
            if (((CreateTable) statement).getType() == CreateTable.Type.BATCH) {
                throw new UnsupportedOperationException("not support batch table");
            }
            else {
                try {
                    createStreamTable((CreateTable) statement, statementNode.getDependOperator().getClassName());
                }
                catch (ClassNotFoundException e) {
                    throw new IllegalStateException("compile failed, not found table connector " + ((CreateTable) statement).getConnector(), e);
                }
            }
        }
        else if (statement instanceof CreateFunction) {
            createFunction((CreateFunction) statement);
        }
        else if (statement instanceof SelectQuery) {
            SelectQuery selectQuery = (SelectQuery) statement;
            String pushDownQuery = selectQuery.getQuery();
            Table table = tableEnv.sqlQuery(pushDownQuery);
            tableEnv.toDataStream(table, Row.class).print();
        }
        else if (statement instanceof InsertInto) {
            InsertInto insertInto = (InsertInto) statement;
            SelectQuery selectQuery = insertInto.getSelectQuery();
            String pushDownQuery = selectQuery.getQuery();
            Table table = tableEnv.sqlQuery(pushDownQuery);
            table.printSchema();
            Sink<DataStream<Row>> dataStreamSink = sinkTables.get(insertInto.getTableName());
            checkState(dataStreamSink != null, "sink table %s not found", insertInto.getTableName());
            //todo: check schema
            dataStreamSink.run(tableEnv.toDataStream(table, Row.class));
        }
        else {
            throw new IllegalArgumentException("this driver class " + statement.getClass() + " have't support!");
        }
    }

    private void createFunction(CreateFunction createFunction)
    {
        Object function = null;
        try {
            Class<?> driver = Class.forName(createFunction.getClassString());
            function = driver.getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new IllegalArgumentException("create function failed: " + createFunction, e);
        }
        if (function instanceof AggregateFunction) {
            tableEnv.createTemporaryFunction(createFunction.getFunctionName(), (AggregateFunction<?, ?>) function);
        }
        else if (function instanceof TableFunction) {
            tableEnv.createTemporaryFunction(createFunction.getFunctionName(), (TableFunction<?>) function);
        }
        else if (function instanceof ScalarFunction) {
            tableEnv.createTemporaryFunction(createFunction.getFunctionName(), (ScalarFunction) function);
        }
        else {
            throw new UnsupportedOperationException("not support function class " + function);
        }
    }

    private void createStreamTable(CreateTable createStream, String className)
            throws ClassNotFoundException
    {
        final String tableName = createStream.getName();
        Schema schema = getTableSchema(createStream);
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
        Function<RealTimeSink, Sink<DataStream<Row>>> sinkCast = realTimeSink -> stream -> stream.addSink(new FlinkSink(realTimeSink, stream.getType()));
        FlinkOperatorFactory<DataStream<Row>> factory = FlinkOperatorFactory.createFactory(iocFactory, sinkCast);
        if (SOURCE == createStream.getType()) {
            Class<?> operatorClass = Class.forName(className);
            Source<DataStream<Row>> source = factory.newInstanceSource(operatorClass, withConfig);
            DataStream<Row> inputStream = source.createSource();
            //---------------------------------------------------
            registerStreamTable(inputStream, tableName, createStream.getWatermark(), createStream.getProctimes());
        }
        else if (SINK == createStream.getType()) {
            Class<?> operatorClass = Class.forName(className);
            Sink<DataStream<Row>> sink = factory.newInstanceSink(operatorClass, withConfig);
            sinkTables.put(tableName, sink);
        }
        else {
            throw new IllegalArgumentException("this Connector " + createStream.getConnector() + " haven't support!");
        }
    }

    private void registerStreamTable(
            DataStream<Row> inputStream,
            String tableName,
            Optional<WaterMark> waterMarkOptional,
            Optional<Proctime> proctimeOptional)
    {
        if (!waterMarkOptional.isPresent()) {
            Table table = tableEnv.fromDataStream(inputStream);
            table.printSchema();
            tableEnv.createTemporaryView(tableName, table);
            return;
        }
        org.apache.flink.table.api.Schema.Builder builder = org.apache.flink.table.api.Schema.newBuilder();

        WaterMark waterMark = waterMarkOptional.get();
        logger.info("createStreamTable Watermark is {}", waterMark);
        builder.columnByExpression(waterMark.getRowTimeName(), String.format("TO_TIMESTAMP_LTZ(%s, 3)", waterMark.getFieldName()));
        builder.watermark(waterMark.getRowTimeName(), String.format("%s - INTERVAL '%s' SECOND", waterMark.getRowTimeName(), waterMark.getOffset() / 1000));
        proctimeOptional.ifPresent(proctime -> builder.columnByExpression(proctime.getName(), "PROCTIME()"));
        org.apache.flink.table.api.Schema schema = builder.build();
        Table table = tableEnv.fromDataStream(inputStream, schema);
        table.printSchema();
        tableEnv.createTemporaryView(tableName, table);
    }
}
