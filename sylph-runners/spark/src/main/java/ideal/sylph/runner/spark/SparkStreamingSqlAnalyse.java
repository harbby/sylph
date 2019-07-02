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

import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.parser.antlr.tree.CreateFunction;
import ideal.sylph.parser.antlr.tree.CreateStreamAsSelect;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.parser.antlr.tree.InsertInto;
import ideal.sylph.parser.antlr.tree.SelectQuery;
import ideal.sylph.parser.antlr.tree.WaterMark;
import ideal.sylph.runner.spark.kafka.SylphKafkaOffset;
import ideal.sylph.runner.spark.sparkstreaming.DStreamUtil;
import ideal.sylph.runner.spark.sparkstreaming.StreamNodeLoader;
import ideal.sylph.spi.ConnectorStore;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag$;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static ideal.sylph.runner.spark.SQLHepler.checkQueryAndTableSinkSchema;
import static ideal.sylph.runner.spark.SQLHepler.getSparkType;
import static ideal.sylph.runner.spark.SQLHepler.getTableSchema;
import static ideal.sylph.runner.spark.SQLHepler.schemaToSparkType;
import static java.util.Objects.requireNonNull;

public class SparkStreamingSqlAnalyse
        implements SqlAnalyse
{
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingSqlAnalyse.class);

    private final JobBuilder builder = new JobBuilder();
    private final StreamingContext ssc;
    private final ConnectorStore connectorStore;
    private final Bean sparkBean;
    private final boolean isCompile;

    public SparkStreamingSqlAnalyse(StreamingContext ssc,
            ConnectorStore connectorStore,
            boolean isCompile)
    {
        this.ssc = ssc;
        this.connectorStore = connectorStore;
        this.sparkBean = binder -> {
            binder.bind(StreamingContext.class, ssc);
            binder.bind(JavaStreamingContext.class, new JavaStreamingContext(ssc));
        };
        this.isCompile = isCompile;
    }

    @Override
    public void finish()
    {
        builder.build();
    }

    @Override
    public void createStreamAsSelect(CreateStreamAsSelect statement)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public void createTable(CreateTable createTable)
    {
        final String tableName = createTable.getName();
        Schema schema = getTableSchema(createTable);
        final StructType tableSparkType = schemaToSparkType(schema);

        final Map<String, Object> withConfig = createTable.getWithConfig();
//        final String driverClass = (String) withConfig.get("type");

        switch (createTable.getType()) {
            case SOURCE:
                SourceContext sourceContext = new SourceContext()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return schema;
                    }

                    @Override
                    public String getSourceTable()
                    {
                        return tableName;
                    }

                    @Override
                    public Map<String, Object> withConfig()
                    {
                        return withConfig;
                    }
                };
                createSourceTable(sourceContext, tableSparkType, createTable.getWatermark());
                return;
            case SINK:
                SinkContext sinkContext = new SinkContext()
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

                    @Override
                    public Map<String, Object> withConfig()
                    {
                        return withConfig;
                    }
                };
                createSinkTable(sinkContext, tableSparkType);
                return;
            case BATCH:
                throw new UnsupportedOperationException("The SparkStreaming engine BATCH TABLE have't support!");
            default:
                throw new IllegalArgumentException("this driver class " + withConfig.get("type") + " have't support!");
        }
    }

    public void createSourceTable(SourceContext sourceContext, StructType tableSparkType, Optional<WaterMark> optionalWaterMark)
    {
        final String driverClass = (String) sourceContext.withConfig().get("type");
        IocFactory iocFactory = IocFactory.create(sparkBean, binder -> binder.bind(SourceContext.class).byInstance(sourceContext));
        StreamNodeLoader loader = new StreamNodeLoader(connectorStore, iocFactory);

        checkState(!optionalWaterMark.isPresent(), "spark streaming not support waterMark");
        UnaryOperator<JavaDStream<Row>> source = loader.loadSource(driverClass, sourceContext.withConfig());
        builder.addSource(source, tableSparkType, sourceContext.getSourceTable());
    }

    public void createSinkTable(SinkContext sinkContext, StructType tableSparkType)
    {
        final String driverClass = (String) sinkContext.withConfig().get("type");
        IocFactory iocFactory = IocFactory.create(sparkBean, binder -> binder.bind(SinkContext.class, sinkContext));
        StreamNodeLoader loader = new StreamNodeLoader(connectorStore, iocFactory);

        UnaryOperator<Dataset<Row>> outputStream = dataSet -> {
            checkQueryAndTableSinkSchema(dataSet.schema(), tableSparkType, sinkContext.getSinkTable());
            loader.loadRDDSink(driverClass, sinkContext.withConfig()).accept(dataSet.javaRDD());
            return null;
        };
        builder.addSink(sinkContext.getSinkTable(), outputStream);
    }

    @Override
    public void createFunction(CreateFunction createFunction)
            throws Exception
    {
        //todo: 需要字节码大法加持
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

    @Override
    public void insertInto(InsertInto insert)
    {
        String tableName = insert.getTableName();
        String query = insert.getQuery();
        builder.addHandler(sparkSession -> {
            Dataset<Row> df = sparkSession.sql(query);
            builder.getSink(tableName).apply(df);
        });
    }

    @Override
    public void selectQuery(SelectQuery statement)
    {
        builder.addHandler(sparkSession -> {
            Dataset<Row> df = sparkSession.sql(statement.toString());
            df.foreach((ForeachFunction<Row>) row -> System.out.println(row.mkString(",")));
            //df.show();
        });
    }

    private class JobBuilder
    {
        private final List<Consumer<SparkSession>> handlers = new ArrayList<>();
        private UnaryOperator<JavaDStream<Row>> source;
        private StructType schema;
        private String sourceTableName;

        private final Map<String, UnaryOperator<Dataset<Row>>> sinks = new HashMap<>();

        public void addSource(UnaryOperator<JavaDStream<Row>> source, StructType schema, String sourceTableName)
        {
            checkState(this.source == null && this.schema == null && this.sourceTableName == null, "sourceTable currently has one and only one, your registered %s", this.sourceTableName);
            this.source = source;
            this.schema = schema;
            this.sourceTableName = sourceTableName;
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
            JavaDStream<Row> inputStream = source.apply(null);
            SparkSession spark = SparkSession.builder().config(inputStream.context().sparkContext().getConf()).getOrCreate();

            if (isCompile) {
                logger.info("isCompile mode will checkDStream()");
                checkDStream(spark, sourceTableName, schema, handlers);
            }

            DStream<?> firstDStream = DStreamUtil.getFirstDStream(inputStream.dstream(), SylphKafkaOffset.class);
            logger.info("source table {}, firstDStream is {}", sourceTableName, firstDStream);
            inputStream.foreachRDD(rdd -> {
                Dataset<Row> df = spark.createDataFrame(rdd, schema);
                df.createOrReplaceTempView(sourceTableName);
                //df.show()
                //if kafka0.10+ if("DirectKafkaInputDStream".equals(firstDStream.getClass().getSimpleName())) {}
                if (firstDStream instanceof SylphKafkaOffset) { //
                    RDD<?> kafkaRdd = DStreamUtil.getFirstRdd(rdd.rdd()); //rdd.dependencies(0).rdd
                    if (kafkaRdd.count() > 0) {
                        handlers.forEach(x -> x.accept(spark)); //执行业务操作
                    }
                    //val offsetRanges = kafkaRdd.asInstanceOf[HasOffsetRanges].offsetRanges
                    //firstDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
                    ((SylphKafkaOffset<?>) firstDStream).commitOffsets(kafkaRdd);
                }
                else {
                    handlers.forEach(x -> x.accept(spark));
                }
            });
        }
    }

    /**
     * 预编译sql 而不是等到运行时，才发现错误
     * Precompiled sql instead of waiting for the runtime to find the error
     */
    private static void checkDStream(
            SparkSession spark,
            String sourceTableName,
            StructType sourceSchema,
            List<Consumer<SparkSession>> handlers
    )
    {
        RDD<Row> rdd = spark.sparkContext().<Row>emptyRDD(ClassTag$.MODULE$.<Row>apply(Row.class));
        Dataset<Row> df = spark.createDataFrame(rdd, sourceSchema);
        df.createOrReplaceTempView(sourceTableName);
        handlers.forEach(x -> x.accept(spark));
        spark.sql("drop view " + sourceTableName);
    }
}
