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

import com.github.harbby.gadtry.base.JavaTypes;
import ideal.sylph.etl.Schema;
import ideal.sylph.parser.antlr.tree.ColumnDefinition;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.parser.antlr.tree.WaterMark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class StreamSqlUtil
{
    private StreamSqlUtil() {}

    static DataStream<Row> checkStream(DataStream<Row> inputStream, RowTypeInfo tableTypeInfo)
    {
        checkState(inputStream.getType() instanceof RowTypeInfo, "DataStream type not is RowTypeInfo");
        RowTypeInfo sourceType = (RowTypeInfo) inputStream.getType();

        List<Integer> indexs = Arrays.stream(tableTypeInfo.getFieldNames())
                .map(fieldName -> {
                    int fieldIndex = sourceType.getFieldIndex(fieldName);
                    checkState(fieldIndex != -1, sourceType + " not with " + fieldName);
                    return fieldIndex;
                })
                .collect(Collectors.toList());
        return inputStream.map(inRow -> Row.of(indexs.stream().map(inRow::getField).toArray()))
                .returns(tableTypeInfo);
    }

    static DataStream<Row> assignWaterMark(
            WaterMark waterMark,
            RowTypeInfo tableTypeInfo,
            DataStream<Row> dataStream)
    {
        String fieldName = waterMark.getFieldName();
        int fieldIndex = tableTypeInfo.getFieldIndex(fieldName);
        checkState(fieldIndex != -1, tableTypeInfo + " not with " + fieldName);
        long offset = waterMark.getOffset();
        return dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>()
        {
            private final long maxOutOfOrderness = offset;  //最大允许的乱序时间
            private long currentMaxTimestamp = Long.MIN_VALUE;

            @Override
            public long extractTimestamp(Row element, long previousElementTimestamp)
            {
                long time = (Long) requireNonNull(element.getField(fieldIndex),
                        String.format("row[%s] field %s[index: %s] is null", element, fieldName, fieldIndex));
                this.currentMaxTimestamp = Math.max(currentMaxTimestamp, time);
                return time;
            }

            @Override
            public Watermark getCurrentWatermark()
            {
                // return the watermark as current highest timestamp minus the out-of-orderness bound
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }
        }).returns(tableTypeInfo);
    }

    public static Schema getTableSchema(CreateTable createStream)
    {
        final List<ColumnDefinition> columns = createStream.getElements();
        Schema.SchemaBuilder builder = Schema.newBuilder();
        columns.forEach(columnDefinition -> {
            builder.add(columnDefinition.getName().getValue(), parserSqlType(columnDefinition.getType()));
        });
        return builder.build();
    }

    private static Type parserSqlType(String type)
    {
        type = type.trim().toLowerCase();
        switch (type) {
            case "varchar":
            case "string":
                return String.class;
            case "integer":
            case "int":
                return Integer.class;
            case "long":
            case "bigint":
                return Long.class;
            case "boolean":
            case "bool":
                return Boolean.class;
            case "double":
                return Double.class;
            case "float":
                return Float.class;
            case "decimal":
                return BigDecimal.class;
            case "time":
                return Time.class;
            case "byte":
                return Byte.class;
            case "timestamp":
                return Timestamp.class;
            case "date":
                return Date.class;
            case "binary":
                return byte[].class; //TypeExtractor.createTypeInfo(byte[].class) or Types.OBJECT_ARRAY(Types.BYTE());
            case "object":
                return Object.class;
            default:
                return defaultArrayOrMap(type);
        }
    }

    private static Type defaultArrayOrMap(String type)
    {
        //final String arrayRegularExpression = "array\\((\\w*?)\\)";
        //final String mapRegularExpression = "map\\((\\w*?),(\\w*?)\\)";
        final String arrayRegularExpression = "(?<=array\\().*(?=\\))";
        final String mapRegularExpression = "(?<=map\\()(\\w*?),(.*(?=\\)))";

        Matcher item = Pattern.compile(arrayRegularExpression).matcher(type);
        while (item.find()) {
            Type arrayType = parserSqlType(item.group(0));
            return JavaTypes.makeArrayType(arrayType);
        }

        item = Pattern.compile(mapRegularExpression).matcher(type);
        while (item.find()) {
            Type keyClass = parserSqlType(item.group(1));
            Type valueClass = parserSqlType(item.group(2));
            return JavaTypes.makeMapType(Map.class, keyClass, valueClass);
        }

        throw new IllegalArgumentException("this TYPE " + type + " have't support!");
    }

    public static RowTypeInfo schemaToRowTypeInfo(Schema schema)
    {
        TypeInformation<?>[] types = schema.getFieldTypes().stream().map(StreamSqlUtil::getFlinkType)
                .toArray(TypeInformation<?>[]::new);
        String[] names = schema.getFieldNames().toArray(new String[0]);
        return new RowTypeInfo(types, names);
    }

    private static TypeInformation<?> getFlinkType(Type type)
    {
        if (type instanceof ParameterizedType && ((ParameterizedType) type).getRawType() == Map.class) {
            Type[] arguments = ((ParameterizedType) type).getActualTypeArguments();
            Type valueType = arguments[1];
            TypeInformation<?> valueInfo = getFlinkType(valueType);
            return new MapTypeInfo<>(TypeExtractor.createTypeInfo(arguments[0]), valueInfo);
        }
        else if (type instanceof ParameterizedType && ((ParameterizedType) type).getRawType() == List.class) {
            TypeInformation<?> typeInformation = getFlinkType(((ParameterizedType) type).getActualTypeArguments()[0]);
            if (typeInformation.isBasicType() && typeInformation != Types.STRING) {
                return Types.PRIMITIVE_ARRAY(typeInformation);
            }
            else {
                return Types.OBJECT_ARRAY(typeInformation);
            }
        }
        else {
            return TypeExtractor.createTypeInfo(type);
        }
    }
}
