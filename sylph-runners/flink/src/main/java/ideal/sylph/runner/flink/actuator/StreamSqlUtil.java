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
import ideal.sylph.parser.tree.CreateStream;
import ideal.sylph.parser.tree.CreateStreamAsSelect;
import ideal.sylph.parser.tree.Statement;
import ideal.sylph.parser.tree.WaterMark;
import ideal.sylph.runner.flink.etl.FlinkNodeLoader;
import ideal.sylph.runner.flink.table.SylphTableSink;
import ideal.sylph.runner.flink.table.SylphTableSource;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static ideal.sylph.parser.tree.CreateStream.Type.SINK;
import static ideal.sylph.parser.tree.CreateStream.Type.SOURCE;

public class StreamSqlUtil
{
    private StreamSqlUtil() {}

    public static void createStreamTableBySql(PipelinePluginManager pluginManager, StreamTableEnvironment tableEnv, SqlParser sqlParser, String sql)
    {
        Statement statement = sqlParser.createStatement(sql);
        if (statement instanceof CreateStreamAsSelect) {
            CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
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
            return;
        }

        CreateStream createStream = (CreateStream) sqlParser.createStatement(sql);
        String tableName = createStream.getName();

        List<ColumnDefinition> columns = createStream.getElements().stream().map(ColumnDefinition.class::cast).collect(Collectors.toList());

        Map<String, String> withConfig = createStream.getProperties().stream()
                .collect(Collectors.toMap(
                        k -> k.getName().getValue(),
                        v -> v.getValue().toString().replace("'", ""))
                );

        Map<String, Object> config = ImmutableMap.<String, Object>builder()
                .putAll(withConfig)
                .put("driver", withConfig.get("type"))
                .put("tableName", tableName)
                .build();
        NodeLoader<StreamTableEnvironment, DataStream<Row>> loader = new FlinkNodeLoader(pluginManager);
        RowTypeInfo rowTypeInfo = parserColumns(columns);
        if (SOURCE == createStream.getType()) {  //Source.class.isAssignableFrom(driver)
            DataStream<Row> inputStream = loader.loadSource(tableEnv, config).apply(null);
            if (createStream.getWatermark().isPresent()) {
                tableEnv.execEnv().setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                inputStream = buildWaterMark(createStream.getWatermark().get(), rowTypeInfo, inputStream);
            }
            SylphTableSource tableSource = new SylphTableSource(rowTypeInfo, inputStream);
            tableEnv.registerTableSource(tableName, tableSource);
        }
        else if (SINK == createStream.getType()) {
            UnaryOperator<DataStream<Row>> outputStream = loader.loadSink(config);
            SylphTableSink tableSink = new SylphTableSink(rowTypeInfo, outputStream);
            tableEnv.registerTableSink(tableName, tableSink.getFieldNames(), tableSink.getFieldTypes(), tableSink);
        }
        else {
            throw new IllegalArgumentException("this driver class " + withConfig.get("type") + " have't support!");
        }
    }

    private static DataStream<Row> buildWaterMark(
            WaterMark waterMark,
            RowTypeInfo rowTypeInfo,
            DataStream<Row> dataStream)
    {
        String fieldName = waterMark.getFieldName();
        int fieldIndex = rowTypeInfo.getFieldIndex(fieldName);

        if (waterMark.getOffset() instanceof WaterMark.RowMaxOffset) {
            long offset = ((WaterMark.RowMaxOffset) waterMark.getOffset()).getOffset();
            return dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>()
            {
                private final long maxOutOfOrderness = offset;  // 5_000L;//最大允许的乱序时间是5s
                private long currentMaxTimestamp = Long.MIN_VALUE;

                @Override
                public long extractTimestamp(Row element, long previousElementTimestamp)
                {
                    long time = (long) element.getField(fieldIndex);
                    this.currentMaxTimestamp = Math.max(currentMaxTimestamp, time);
                    return time;
                }

                @Nullable
                @Override
                public Watermark getCurrentWatermark()
                {
                    // return the watermark as current highest timestamp minus the out-of-orderness bound
                    return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                }
            }).returns(rowTypeInfo);
        }
        else if (waterMark.getOffset() instanceof WaterMark.SystemOffset) {
            long offset = ((WaterMark.SystemOffset) waterMark.getOffset()).getOffset();
            return dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>()
            {
                private final long maxOutOfOrderness = offset;  // 5_000L;//最大允许的乱序时间是5s

                @Override
                public long extractTimestamp(Row element, long previousElementTimestamp)
                {
                    long time = (long) element.getField(fieldIndex);
                    return time;
                }

                @Nullable
                @Override
                public Watermark getCurrentWatermark()
                {
                    return new Watermark(System.currentTimeMillis() - maxOutOfOrderness);
                }
            }).returns(rowTypeInfo);
        }
        else {
            throw new UnsupportedOperationException("this " + waterMark + " have't support!");
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
