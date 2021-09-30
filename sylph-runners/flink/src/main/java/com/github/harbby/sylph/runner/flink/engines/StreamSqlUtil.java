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

import com.github.harbby.gadtry.base.JavaTypes;
import com.github.harbby.sylph.api.Schema;
import com.github.harbby.sylph.parser.tree.ColumnDefinition;
import com.github.harbby.sylph.parser.tree.CreateTable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StreamSqlUtil
{
    private StreamSqlUtil() {}

    public static Schema getTableSchema(CreateTable createStream)
    {
        final List<ColumnDefinition> columns = createStream.getElements();
        Schema.SchemaBuilder builder = Schema.newBuilder();
        columns.forEach(columnDefinition -> {
            builder.add(columnDefinition.getName().getValue(), parserSqlType(columnDefinition.getType()), columnDefinition.getExtend().orElse(null));
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
                return byte[].class;
            case "object":
                return Object.class;
            default:
                return defaultArrayOrMap(type);
        }
    }

    private static Type defaultArrayOrMap(String type)
    {
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
