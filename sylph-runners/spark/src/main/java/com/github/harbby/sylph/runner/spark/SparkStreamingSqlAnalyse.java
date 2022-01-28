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

import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.sylph.api.Schema;
import com.github.harbby.sylph.api.Sink;
import com.github.harbby.sylph.api.Source;
import com.github.harbby.sylph.api.TableContext;
import com.github.harbby.sylph.parser.tree.CreateFunction;
import com.github.harbby.sylph.parser.tree.CreateStreamAsSelect;
import com.github.harbby.sylph.parser.tree.CreateTable;
import com.github.harbby.sylph.parser.tree.InsertInto;
import com.github.harbby.sylph.parser.tree.SelectQuery;
import com.github.harbby.sylph.parser.tree.WaterMark;
import com.github.harbby.sylph.runner.spark.kafka.SylphKafkaOffset;
import com.github.harbby.sylph.runner.spark.sparkstreaming.DStreamUtil;
import com.github.harbby.sylph.runner.spark.sparkstreaming.SparkStreamingOperatorFactory;
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
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.sylph.runner.spark.SQLHepler.checkQueryAndTableSinkSchema;
import static com.github.harbby.sylph.runner.spark.SQLHepler.getSparkType;
import static com.github.harbby.sylph.runner.spark.SQLHepler.getTableSchema;
import static com.github.harbby.sylph.runner.spark.SQLHepler.schemaToSparkType;
import static java.util.Objects.requireNonNull;

public class SparkStreamingSqlAnalyse
        implements SqlAnalyse
{
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingSqlAnalyse.class);

    private final JobBuilder builder = new JobBuilder();
    private final StreamingContext ssc;
    private final Bean sparkBean;
    private final boolean isCompile;

    public SparkStreamingSqlAnalyse(StreamingContext ssc,
            boolean isCompile)
    {
        this.ssc = ssc;
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
    public void createTable(CreateTable createTable, String className)
            throws ClassNotFoundException
    {
        final String tableName = createTable.getName();
        Schema schema = getTableSchema(createTable);
        final StructType tableSparkType = schemaToSparkType(schema);
        final String connector = createTable.getConnector();
        final Map<String, Object> withConfig = createTable.getWithProperties();
        TableContext tableContext = new TableContext()
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
        };
        switch (createTable.getType()) {
            case SOURCE:
                createSourceTable(tableContext, tableSparkType, createTable.getWatermark(), className);
                return;
            case SINK:
                createSinkTable(tableContext, tableSparkType, className);
                return;
            case BATCH:
                throw new UnsupportedOperationException("The SparkStreaming engine BATCH TABLE haven't support!");
            default:
                throw new IllegalArgumentException("this Connector " + connector + " haven't support!");
        }
    }

    public void createSourceTable(TableContext sourceContext,
            StructType tableSparkType, Optional<WaterMark> optionalWaterMark,
            String className)
            throws ClassNotFoundException
    {
        IocFactory iocFactory = IocFactory.create(sparkBean, binder -> binder.bind(TableContext.class).byInstance(sourceContext));
        SparkStreamingOperatorFactory loader = new SparkStreamingOperatorFactory(iocFactory);

        checkState(!optionalWaterMark.isPresent(), "spark streaming not support waterMark");
        Source<JavaDStream<Row>> source = loader.createSource(Class.forName(className), sourceContext.withConfig());
        builder.addSource(source, tableSparkType, sourceContext.getTableName());
    }

    public void createSinkTable(TableContext sinkContext, StructType tableSparkType, String className)
            throws ClassNotFoundException
    {
        IocFactory iocFactory = IocFactory.create(sparkBean, binder -> binder.bind(TableContext.class, sinkContext));
        SparkStreamingOperatorFactory loader = new SparkStreamingOperatorFactory(iocFactory);
        Class<?> driverClass = Class.forName(className);
        Sink<Dataset<Row>> outputStream = dataSet -> {
            checkQueryAndTableSinkSchema(dataSet.schema(), tableSparkType, sinkContext.getTableName());
            loader.loadRDDSink(driverClass, sinkContext.withConfig()).run(dataSet.javaRDD());
        };
        builder.addSink(sinkContext.getTableName(), outputStream);
    }

    @Override
    public void createFunction(CreateFunction createFunction)
            throws Exception
    {
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

        builder.addHandler(sparkSession -> {
            sparkSession.udf().registerJava(functionName, functionClass.getName(), returnType);
        });
        //throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public void insertInto(InsertInto insert)
    {
        String tableName = insert.getTableName();
        String query = insert.getSelectQuery().getQuery();
        builder.addHandler(sparkSession -> {
            Dataset<Row> df = sparkSession.sql(query);
            builder.getSink(tableName).run(df);
        });
    }

    @Override
    public void selectQuery(SelectQuery statement)
    {
        builder.addHandler(sparkSession -> {
            Dataset<Row> df = sparkSession.sql(statement.getQuery());
            df.foreach((ForeachFunction<Row>) row -> System.out.println(row.mkString(",")));
            //df.show();
        });
    }

    private class JobBuilder
    {
        private final List<Consumer<SparkSession>> handlers = new ArrayList<>();
        private Source<JavaDStream<Row>> source;
        private StructType schema;
        private String sourceTableName;

        private final Map<String, Sink<Dataset<Row>>> sinks = new HashMap<>();

        public void addSource(Source<JavaDStream<Row>> source, StructType schema, String sourceTableName)
        {
            checkState(this.source == null && this.schema == null && this.sourceTableName == null, "sourceTable currently has one and only one, your registered %s", this.sourceTableName);
            this.source = source;
            this.schema = schema;
            this.sourceTableName = sourceTableName;
        }

        public void addSink(String name, Sink<Dataset<Row>> sink)
        {
            checkState(sinks.put(name, sink) == null, "sink table " + name + " already exists");
        }

        public Sink<Dataset<Row>> getSink(String name)
        {
            return requireNonNull(sinks.get(name), "sink name not find");
        }

        public void addHandler(Consumer<SparkSession> handler)
        {
            handlers.add(handler);
        }

        public void build()
        {
            JavaDStream<Row> inputStream = source.createSource();
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
                        handlers.forEach(x -> x.accept(spark)); //user code
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
     * Precompiled sql instead of waiting for the runtime to find the error
     */
    private static void checkDStream(
            SparkSession spark,
            String sourceTableName,
            StructType sourceSchema,
            List<Consumer<SparkSession>> handlers)
    {
        RDD<Row> rdd = spark.sparkContext().<Row>emptyRDD(ClassTag$.MODULE$.<Row>apply(Row.class));
        Dataset<Row> df = spark.createDataFrame(rdd, sourceSchema);
        df.createOrReplaceTempView(sourceTableName);
        handlers.forEach(x -> x.accept(spark));
        spark.sql("drop view " + sourceTableName);
    }
}
