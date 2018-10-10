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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.parser.SqlParser;
import ideal.sylph.parser.tree.ColumnDefinition;
import ideal.sylph.parser.tree.CreateFunction;
import ideal.sylph.parser.tree.CreateStream;
import ideal.sylph.parser.tree.CreateStreamAsSelect;
import ideal.sylph.parser.tree.Statement;
import ideal.sylph.runner.flink.etl.FlinkNodeLoader;
import ideal.sylph.runner.flink.table.SylphTableSink;
import ideal.sylph.spi.Binds;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static ideal.sylph.parser.tree.CreateStream.Type.SINK;
import static ideal.sylph.parser.tree.CreateStream.Type.SOURCE;
import static ideal.sylph.runner.flink.actuator.StreamSqlUtil.buildWaterMark;
import static ideal.sylph.runner.flink.actuator.StreamSqlUtil.checkStream;

class StreamSqlBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamEtlActuator.class);

    private final PipelinePluginManager pluginManager;
    private final StreamTableEnvironment tableEnv;
    private final SqlParser sqlParser;

    StreamSqlBuilder(
            StreamTableEnvironment tableEnv,
            PipelinePluginManager pluginManager,
            SqlParser sqlParser
    )
    {
        this.pluginManager = pluginManager;
        this.tableEnv = tableEnv;
        this.sqlParser = sqlParser;
    }

    void buildStreamBySql(String sql)
    {
        if (sql.toLowerCase().contains("create ") &&
                (sql.toLowerCase().contains(" table ") || sql.toLowerCase().contains(" function "))
        ) {
            Statement statement = sqlParser.createStatement(sql);
            if (statement instanceof CreateStreamAsSelect) {
                createStreamTableAsSelect((CreateStreamAsSelect) statement);
            }
            else if (statement instanceof CreateStream) {
                createStreamTable((CreateStream) statement);
            }
            else if (statement instanceof CreateFunction) {
                createFunction((CreateFunction) statement);
            }
            else {
                throw new IllegalArgumentException("this driver class " + statement.getClass() + " have't support!");
            }
        }
        else {
            tableEnv.sqlUpdate(sql);
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
            tableEnv.registerFunction(createFunction.getFunctionName(), (AggregateFunction) function);
        }
        else if (function instanceof TableFunction) {
            tableEnv.registerFunction(createFunction.getFunctionName(), (TableFunction) function);
        }
        else if (function instanceof ScalarFunction) {
            tableEnv.registerFunction(createFunction.getFunctionName(), (ScalarFunction) function);
        }
    }

    private void createStreamTable(CreateStream createStream)
    {
        final String tableName = createStream.getName();
        final List<ColumnDefinition> columns = createStream.getElements().stream().map(ColumnDefinition.class::cast).collect(Collectors.toList());

        final Map<String, String> withConfig = createStream.getProperties().stream()
                .collect(Collectors.toMap(
                        k -> k.getName().getValue(),
                        v -> v.getValue().toString().replace("'", "")));
        final Map<String, Object> config = ImmutableMap.copyOf(withConfig);
        final String driverClass = withConfig.get("type");

        final Binds binds = Binds.builder()
                .put(org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class, tableEnv.execEnv())
                .put(org.apache.flink.table.api.StreamTableEnvironment.class, tableEnv)
                .put(org.apache.flink.table.api.java.StreamTableEnvironment.class, tableEnv)
                //.put(org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.class, null) // execEnv
                //.put(org.apache.flink.table.api.scala.StreamTableEnvironment.class, null)  // tableEnv
                .build();
        NodeLoader<DataStream<Row>> loader = new FlinkNodeLoader(pluginManager, binds);
        RowTypeInfo tableTypeInfo = parserColumns(columns);
        if (SOURCE == createStream.getType()) {  //Source.class.isAssignableFrom(driver)
            DataStream<Row> inputStream = checkStream(loader.loadSource(driverClass, config).apply(null), tableTypeInfo);
            //---------------------------------------------------
            createStream.getWatermark().ifPresent(waterMark -> {
                logger.info("createStreamTable Watermark is {}", waterMark);
                tableEnv.execEnv().setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                DataStream<Row> waterMarkStream = buildWaterMark(waterMark, tableTypeInfo, inputStream);
                String fields = String.join(",", ImmutableList.<String>builder()
                        .add(tableTypeInfo.getFieldNames())
                        .add(waterMark.getFieldForName() + ".rowtime")
                        .build());
                tableEnv.registerDataStream(tableName, waterMarkStream, fields);
            });
            if (!createStream.getWatermark().isPresent()) {
                tableEnv.registerDataStream(tableName, inputStream);
            }
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

    private void createStreamTableAsSelect(CreateStreamAsSelect createStreamAsSelect)
    {
        Table table = tableEnv.sqlQuery(createStreamAsSelect.getViewSql());
        RowTypeInfo rowTypeInfo = (RowTypeInfo) table.getSchema().toRowType();

        createStreamAsSelect.getWatermark().ifPresent(waterMark -> {
            tableEnv.execEnv().setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            DataStream<Row> inputStream = buildWaterMark(waterMark, rowTypeInfo, tableEnv.toAppendStream(table, Row.class));
            String fields = String.join(",", ImmutableList.<String>builder()
                    .add(rowTypeInfo.getFieldNames())
                    .add(waterMark.getFieldForName() + ".rowtime")
                    .build());
            tableEnv.registerDataStream(createStreamAsSelect.getName(), inputStream, fields);
        });
        if (!createStreamAsSelect.getWatermark().isPresent()) {
            tableEnv.registerTable(createStreamAsSelect.getName(), table);
        }
    }

    private static RowTypeInfo parserColumns(List<ColumnDefinition> columns)
    {
        String[] fieldNames = columns.stream().map(columnDefinition -> columnDefinition.getName().getValue())
                .toArray(String[]::new);

        TypeInformation[] fieldTypes = columns.stream().map(columnDefinition -> parserSqlType(columnDefinition.getType()))
                .toArray(TypeInformation[]::new);

        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    private static TypeInformation<?> parserSqlType(String type)
    {
        switch (type) {
            case "varchar":
            case "string":
                return Types.STRING();
            case "integer":
            case "int":
                return Types.INT();
            case "long":
            case "bigint":
                return Types.LONG();
            case "boolean":
            case "bool":
                return Types.BOOLEAN();
            case "double":
                return Types.DOUBLE();
            case "float":
                return Types.FLOAT();
            case "byte":
                return Types.BYTE();
            case "timestamp":
                return Types.SQL_TIMESTAMP();
            case "date":
                return Types.SQL_DATE();
            case "binary":
                return TypeExtractor.createTypeInfo(byte[].class); //Types.OBJECT_ARRAY(Types.BYTE());
            default:
                throw new IllegalArgumentException("this TYPE " + type + " have't support!");
        }
    }
}
