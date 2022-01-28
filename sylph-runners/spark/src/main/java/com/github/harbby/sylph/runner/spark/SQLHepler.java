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
package com.github.harbby.sylph.runner.spark;

import com.github.harbby.gadtry.base.JavaTypes;
import com.github.harbby.sylph.api.Schema;
import com.github.harbby.sylph.parser.tree.ColumnDefinition;
import com.github.harbby.sylph.parser.tree.CreateFunction;
import com.github.harbby.sylph.parser.tree.CreateStreamAsSelect;
import com.github.harbby.sylph.parser.tree.CreateTable;
import com.github.harbby.sylph.parser.tree.InsertInto;
import com.github.harbby.sylph.parser.tree.SelectQuery;
import com.github.harbby.sylph.parser.tree.Statement;
import com.github.harbby.sylph.spi.job.SqlJobParser;
import org.apache.spark.SparkException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.Throwables.throwThrowable;

public class SQLHepler
{
    private SQLHepler() {}

    public static void buildSql(SqlAnalyse analyse, SqlJobParser flow)
            throws Exception
    {
        for (SqlJobParser.StatementNode statementNode : flow.getTree()) {
            Statement statement = statementNode.getStatement();
            if (statement instanceof CreateStreamAsSelect) {
                analyse.createStreamAsSelect((CreateStreamAsSelect) statement);
            }
            else if (statement instanceof CreateTable) {
                analyse.createTable((CreateTable) statement, statementNode.getDependOperator().getClassName());
            }
            else if (statement instanceof CreateFunction) {
                analyse.createFunction((CreateFunction) statement);
            }
            else if (statement instanceof InsertInto) {
                analyse.insertInto((InsertInto) statement);
            }
            else if (statement instanceof SelectQuery) {
                analyse.selectQuery((SelectQuery) statement);
            }
            else {
                throw new IllegalArgumentException("this driver class " + statement.getClass() + " have't support!");
            }
        }
        analyse.finish();
    }

    static void checkQueryAndTableSinkSchema(StructType querySchema, StructType tableSinkSchema, String tableName)
    {
        if (querySchema.size() != tableSinkSchema.size()) {
            try {
                throw new SparkException("Field types of query result size:" + querySchema.size() + " and registered TableSink " + tableName + " size: " + tableSinkSchema.size() + " do not match." +
                        "\nQuery result schema: " + structTypeToString(querySchema) +
                        "\nTableSink schema:    " + structTypeToString(tableSinkSchema));
            }
            catch (SparkException e) {
                throwThrowable(e);
            }
        }

        for (int i = 0; i < querySchema.size(); i++) {
            StructField queryField = querySchema.apply(i);
            StructField tableSinkField = tableSinkSchema.apply(i);
            if (DataTypes.NullType.equals(queryField.dataType())) {
                continue;
            }

            if (!queryField.dataType().equals(tableSinkField.dataType())) {
                try {
                    throw new SparkException("Field types of query result  and registered TableSink " + tableName + " do not match." +
                            "\nqueryField " + queryField + " type not is tableSinkField " + tableSinkField + " type" +
                            "\nQuery result schema: " + structTypeToString(querySchema) +
                            "\nTableSink schema:    " + structTypeToString(tableSinkSchema));
                }
                catch (SparkException e) {
                    throwThrowable(e);
                }
            }
        }
    }

    private static String structTypeToString(StructType structType)
    {
        return Arrays.stream(structType.fields()).map(x -> x.name() + ": " +
                        x.dataType().catalogString())
                .collect(Collectors.toList())
                .toString();
    }

    public static StructType schemaToSparkType(Schema schema)
    {
        StructField[] structFields = schema.getFields().stream().map(field ->
                StructField.apply(field.getName(), getSparkType(field.getJavaType()), true, Metadata.empty())
        ).toArray(StructField[]::new);

        StructType structType = new StructType(structFields);
        return structType;
    }

    static DataType getSparkType(Type type)
    {
        if (type instanceof ParameterizedType && ((ParameterizedType) type).getRawType() == Map.class) {
            Type[] arguments = ((ParameterizedType) type).getActualTypeArguments();

            return DataTypes.createMapType(getSparkType(arguments[0]), getSparkType(arguments[1]));
        }
        else if (type instanceof ParameterizedType && ((ParameterizedType) type).getRawType() == List.class) {
            DataType dataType = getSparkType(((ParameterizedType) type).getActualTypeArguments()[0]);

            return DataTypes.createArrayType(dataType);
        }
        else {
            if (type == String.class) {
                return DataTypes.StringType;
            }
            else if (type == int.class || type == Integer.class) {
                return DataTypes.IntegerType;
            }
            else if (type == long.class || type == Long.class) {
                return DataTypes.LongType;
            }
            else if (type == boolean.class || type == Boolean.class) {
                return DataTypes.BooleanType;
            }
            else if (type == double.class || type == Double.class) {
                return DataTypes.DoubleType;
            }
            else if (type == float.class || type == Float.class) {
                return DataTypes.FloatType;
            }
            else if (type == byte.class || type == Byte.class) {
                return DataTypes.ByteType;
            }
            else if (type == Timestamp.class) {
                return DataTypes.TimestampType;
            }
            else if (type == Date.class) {
                return DataTypes.DateType;
            }
            else if (type == byte[].class || type == Byte[].class) {
                return DataTypes.BinaryType;
            }
            else {
                throw new IllegalArgumentException("this TYPE " + type + " have't support!");
            }
        }
    }

    public static Schema getTableSchema(CreateTable createTable)
    {
        final List<ColumnDefinition> columns = createTable.getElements();
        Schema.SchemaBuilder builder = Schema.newBuilder();
        columns.forEach(column -> builder.add(
                column.getName().getValue(),
                parserSqlType(column.getType()),
                column.getExtend().orElse(null)));
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
                return int.class;
            case "long":
            case "bigint":
                return long.class;
            case "boolean":
            case "bool":
                return boolean.class;
            case "double":
                return double.class;
            case "float":
                return float.class;
            case "byte":
                return byte.class;
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
            return JavaTypes.make(List.class, new Type[] {arrayType}, null);
        }

        item = Pattern.compile(mapRegularExpression).matcher(type);
        while (item.find()) {
            Type keyClass = parserSqlType(item.group(1));
            Type valueClass = parserSqlType(item.group(2));
            return JavaTypes.make(Map.class, new Type[] {keyClass, valueClass}, null);
        }

        throw new IllegalArgumentException("this TYPE " + type + " have't support!");
    }
}
