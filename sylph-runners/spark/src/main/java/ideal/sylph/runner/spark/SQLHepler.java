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
package ideal.sylph.runner.spark;

import com.github.harbby.gadtry.base.JavaTypes;
import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.tree.ColumnDefinition;
import ideal.sylph.parser.antlr.tree.CreateFunction;
import ideal.sylph.parser.antlr.tree.CreateStreamAsSelect;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.parser.antlr.tree.InsertInto;
import ideal.sylph.parser.antlr.tree.SelectQuery;
import ideal.sylph.parser.antlr.tree.Statement;
import ideal.sylph.runner.spark.etl.sparkstreaming.StreamNodeLoader;
import ideal.sylph.spi.job.SqlFlow;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static ideal.sylph.parser.antlr.tree.CreateTable.Type.SINK;
import static ideal.sylph.parser.antlr.tree.CreateTable.Type.SOURCE;
import static java.util.Objects.requireNonNull;

public class SQLHepler
{

    public static void buildSql(StreamingContext ssc, final PipelinePluginManager pluginManager, String jobId, SqlFlow flow)
            throws Exception
    {
        AntlrSqlParser parser = new AntlrSqlParser();
        JobBuilder builder = new JobBuilder();

        for (String sql : flow.getSqlSplit()) {
            Statement statement = parser.createStatement(sql);

            if (statement instanceof CreateStreamAsSelect) {
                throw new UnsupportedOperationException("this method have't support!");
            }
            else if (statement instanceof CreateTable) {
                if (((CreateTable) statement).getType() == CreateTable.Type.BATCH) {
                    throw new UnsupportedOperationException("this method have't support!");
                }
                else {
                    createStreamTable(builder, ssc, pluginManager, (CreateTable) statement);
                }
            }
            else if (statement instanceof CreateFunction) {
                //todo: 需要字节码大法加持
                CreateFunction createFunction = (CreateFunction) statement;
                Class<?> functionClass = Class.forName(createFunction.getClassString());
                String functionName = createFunction.getFunctionName();
                List<ParameterizedType> funcs = Arrays.stream(functionClass.getGenericInterfaces())
                        .filter(x -> x instanceof ParameterizedType)
                        .map(ParameterizedType.class::cast)
                        .collect(Collectors.toList());
                //this check copy @see: org.apache.spark.sql.UDFRegistration#registerJava
                checkState(!funcs.isEmpty(), "UDF class " + functionClass + " doesn't implement any UDF interface");
                checkState(funcs.size() < 2, "It is invalid to implement multiple UDF interfaces, UDF class " + functionClass);
                Type[] types = funcs.get(0).getActualTypeArguments();
                DataType returnType = getSparkType(types[types.length - 1]);

//                UDF1<Object, Object> udf1 = (a) -> null;
//                UDF2<Object, Object, Object> udf2 = (a, b) -> null;
//
//                UDF2 ae = AopFactory.proxyInstance(udf2)
//                        .byClass(UDF2.class)
//                        .whereMethod((java.util.function.Function<MethodInfo, Boolean> & Serializable) methodInfo -> methodInfo.getName().equals("call"))
//                        .around((Function<ProxyContext, Object> & Serializable) proxyContext -> {
//                            TimeUtil.FromUnixTime fromUnixTime = (TimeUtil.FromUnixTime) functionClass.newInstance();
//                            Method method = functionClass.getMethod("eval", proxyContext.getInfo().getParameterTypes());
//                            return method.invoke(fromUnixTime, proxyContext.getArgs());
//                        });

                builder.addHandler(sparkSession -> {
                    sparkSession.udf().registerJava(functionName, functionClass.getName(), returnType);
                });
                //throw new UnsupportedOperationException("this method have't support!");
            }
            else if (statement instanceof InsertInto) {
                InsertInto insert = (InsertInto) statement;
                String tableName = insert.getTableName();
                String query = insert.getQuery();
                builder.addHandler(sparkSession -> {
                    Dataset<Row> df = sparkSession.sql(query);
                    builder.getSink(tableName).apply(df);
                });
            }
            else if (statement instanceof SelectQuery) {
                builder.addHandler(sparkSession -> {
                    Dataset<Row> df = sparkSession.sql(statement.toString());
                    df.show();
                });
            }
            else {
                throw new IllegalArgumentException("this driver class " + statement.getClass() + " have't support!");
            }
        }

        builder.build();
    }

    private static class JobBuilder
    {
        private final List<Consumer<SparkSession>> handlers = new ArrayList<>();
        private UnaryOperator<DStream<Row>> source;
        private StructType schema;

        private final Map<String, UnaryOperator<Dataset<Row>>> sinks = new HashMap<>();

        public void addSource(UnaryOperator<DStream<Row>> source, StructType schema)
        {
            this.source = source;
            this.schema = schema;
        }

        public void addSink(String name, UnaryOperator<Dataset<Row>> sink)
        {
            checkState(sinks.put(name, sink) == null, "sink table " + name + " already exists");
        }

        public UnaryOperator<Dataset<Row>> getSink(String name)
        {
            return requireNonNull(sinks.get(name), "sink name not find");
        }

        public void addHandler(Consumer<SparkSession> handler)
        {
            handlers.add(handler);
        }

        public void build()
        {
            DStream<Row> inputStream = source.apply(null);
            SqlUtil.registerStreamTable(inputStream, "map_source", schema, handlers);
        }
    }

    private static void createStreamTable(JobBuilder builder, StreamingContext ssc, PipelinePluginManager pluginManager, CreateTable createStream)
    {
        final String tableName = createStream.getName();
        Schema schema = getTableSchema(createStream);

        final Map<String, Object> withConfig = createStream.getWithConfig();
        final Map<String, Object> config = ImmutableMap.copyOf(withConfig);
        final String driverClass = (String) withConfig.get("type");

        Bean bean = binder -> {};
        if (SINK == createStream.getType()) {
            bean = binder -> binder.bind(SinkContext.class, new SinkContext()
            {
                @Override
                public Schema getSchema()
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
                @Override
                public Schema getSchema()
                {
                    return schema;
                }
            });
        }
        Bean sparkBean = binder -> {
            binder.bind(StreamingContext.class, ssc);
            binder.bind(JavaStreamingContext.class, new JavaStreamingContext(ssc));
        };
        IocFactory iocFactory = IocFactory.create(bean, sparkBean);
        StreamNodeLoader loader = new StreamNodeLoader(pluginManager, iocFactory);

        final StructType tableSparkType = schemaToSparkType(schema);
        if (SOURCE == createStream.getType()) {  //Source.class.isAssignableFrom(driver)
            checkState(!createStream.getWatermark().isPresent(), "spark streaming not support waterMark");
            UnaryOperator<DStream<Row>> source = loader.loadSource(driverClass, config);
            builder.addSource(source, tableSparkType);
        }
        else if (SINK == createStream.getType()) {
            UnaryOperator<Dataset<Row>> outputStream = dataSet -> {
                checkQueryAndTableSinkSchema(dataSet.schema(), tableSparkType, tableName);
                return loader.loadRDDSink(driverClass, config).apply(dataSet);
            };
            builder.addSink(tableName, outputStream);
        }
        else {
            throw new IllegalArgumentException("this driver class " + withConfig.get("type") + " have't support!");
        }
    }

    private static void checkQueryAndTableSinkSchema(StructType querySchema, StructType tableSinkSchema, String tableName)
    {
        if (!Arrays.stream(querySchema.fields()).map(StructField::dataType).collect(Collectors.toList()).equals(
                Arrays.stream(tableSinkSchema.fields()).map(StructField::dataType).collect(Collectors.toList())
        )) {
            throw new AssertionError("Field types of query result and registered TableSink " + tableName + " do not match.\n" +
                    "Query result schema: " + structTypeToString(querySchema) +
                    "\nTableSink schema:    " + structTypeToString(tableSinkSchema));
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

    private static DataType getSparkType(Type type)
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
