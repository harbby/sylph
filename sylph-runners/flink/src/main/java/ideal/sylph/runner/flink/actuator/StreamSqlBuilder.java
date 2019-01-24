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
package ideal.sylph.runner.flink.actuator;

import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.parser.antlr.AntlrSqlParser;
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
import ideal.sylph.runner.flink.table.SylphTableSink;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
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
import java.util.stream.Collectors;

import static ideal.sylph.parser.antlr.tree.CreateTable.Type.SINK;
import static ideal.sylph.parser.antlr.tree.CreateTable.Type.SOURCE;
import static ideal.sylph.runner.flink.actuator.StreamSqlUtil.buildSylphSchema;
import static ideal.sylph.runner.flink.actuator.StreamSqlUtil.buildWaterMark;
import static ideal.sylph.runner.flink.actuator.StreamSqlUtil.checkStream;
import static ideal.sylph.runner.flink.actuator.StreamSqlUtil.getTableRowTypeInfo;

public class StreamSqlBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamEtlActuator.class);

    private final PipelinePluginManager pluginManager;
    private final StreamTableEnvironment tableEnv;
    private final AntlrSqlParser sqlParser;

    private final List<CreateTable> batchTables = new ArrayList<>();

    public StreamSqlBuilder(
            StreamTableEnvironment tableEnv,
            PipelinePluginManager pluginManager,
            AntlrSqlParser sqlParser
    )
    {
        this.pluginManager = pluginManager;
        this.tableEnv = tableEnv;
        this.sqlParser = sqlParser;
    }

    public void buildStreamBySql(String sql)
    {
        FlinkSqlParser flinkSqlParser = FlinkSqlParser.builder()
                .setTableEnv(tableEnv)
                .setBatchPluginManager(pluginManager)
                .build();
        Statement statement = sqlParser.createStatement(sql);

        if (statement instanceof CreateStreamAsSelect) {
            CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
            Table table = tableEnv.sqlQuery(createStreamAsSelect.getViewSql());
            RowTypeInfo rowTypeInfo = (RowTypeInfo) table.getSchema().toRowType();
            DataStream<Row> stream = tableEnv.toAppendStream(table, Row.class);
            stream.getTransformation().setOutputType(rowTypeInfo);

            registerStreamTable(stream, createStreamAsSelect.getName(), createStreamAsSelect.getWatermark(), ImmutableList.of());
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
        else if (statement instanceof InsertInto || statement instanceof SelectQuery) {
            flinkSqlParser.parser(sql, ImmutableList.copyOf(batchTables));
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
        RowTypeInfo tableTypeInfo = getTableRowTypeInfo(createStream);

        final Map<String, Object> withConfig = createStream.getWithConfig();
        final Map<String, Object> config = ImmutableMap.copyOf(withConfig);
        final String driverClass = (String) withConfig.get("type");

        Bean bean = binder -> {};
        if (SINK == createStream.getType()) {
            bean = binder -> binder.bind(SinkContext.class, new SinkContext()
            {
                private final ideal.sylph.etl.Row.Schema schema = buildSylphSchema(tableTypeInfo);

                @Override
                public ideal.sylph.etl.Row.Schema getSchema()
                {
                    return schema;
                }

                @Override
                public String getSinkTable()
                {
                    return tableName;
                }
            });
        }
        else if (SOURCE == createStream.getType()) {
            bean = binder -> binder.bind(SourceContext.class, new SourceContext()
            {
                private final ideal.sylph.etl.Row.Schema schema = buildSylphSchema(tableTypeInfo);

                @Override
                public ideal.sylph.etl.Row.Schema getSchema()
                {
                    return schema;
                }

                @Override
                public String getSinkTable()
                {
                    return tableName;
                }
            });
        }

        final IocFactory iocFactory = IocFactory.create(new FlinkBean(tableEnv), bean);
        NodeLoader<DataStream<Row>> loader = new FlinkNodeLoader(pluginManager, iocFactory);

        if (SOURCE == createStream.getType()) {  //Source.class.isAssignableFrom(driver)
            DataStream<Row> inputStream = checkStream(loader.loadSource(driverClass, config).apply(null), tableTypeInfo);
            //---------------------------------------------------
            registerStreamTable(inputStream, tableName, createStream.getWatermark(), createStream.getProctimes());
        }
        else if (SINK == createStream.getType()) {
            UnaryOperator<DataStream<Row>> outputStream = loader.loadSink(driverClass, config);
            SylphTableSink tableSink = new SylphTableSink(tableTypeInfo, outputStream);
            tableEnv.registerTableSink(tableName, tableSink.getFieldNames(), tableSink.getFieldTypes(), tableSink);
        }
        else {
            throw new IllegalArgumentException("this driver class " + withConfig.get("type") + " have't support!");
        }
    }

    private void registerStreamTable(
            DataStream<Row> inputStream,
            String tableName,
            Optional<WaterMark> waterMarkOptional,
            List<Proctime> proctimes)
    {
        RowTypeInfo tableTypeInfo = (RowTypeInfo) inputStream.getType();
        waterMarkOptional.ifPresent(waterMark -> {
            logger.info("createStreamTable Watermark is {}", waterMark);
            tableEnv.execEnv().setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            DataStream<Row> waterMarkStream = buildWaterMark(waterMark, tableTypeInfo, inputStream);
            String fields = String.join(",", ImmutableList.<String>builder()
                    .add(tableTypeInfo.getFieldNames())
                    .add(waterMark.getFieldForName() + ".rowtime")
                    .addAll(proctimes.stream().map(x -> x.getName().getValue() + ".proctime").collect(Collectors.toList()))
                    .build());
            tableEnv.registerDataStream(tableName, waterMarkStream, fields);
        });
        if (!waterMarkOptional.isPresent()) {
            String fields = String.join(",", ImmutableList.<String>builder()
                    .add(tableTypeInfo.getFieldNames())
                    .addAll(proctimes.stream().map(x -> x.getName().getValue() + ".proctime").collect(Collectors.toList()))
                    .build());
            tableEnv.registerDataStream(tableName, inputStream, fields);
        }
    }
}
