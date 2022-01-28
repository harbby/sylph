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
import com.github.harbby.sylph.runner.spark.structured.StructuredNodeLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.sylph.runner.spark.SQLHepler.getSparkType;
import static com.github.harbby.sylph.runner.spark.SQLHepler.getTableSchema;
import static com.github.harbby.sylph.runner.spark.SQLHepler.schemaToSparkType;

public class StructuredStreamingSqlAnalyse
        implements SqlAnalyse
{
    private final SparkSession sparkSession;
    private final Map<String, Sink<Dataset<Row>>> sinks = new HashMap<>();
    private final Bean sparkBean;
    private final boolean isCompile;

    //todo: use config
    private final String checkpointLocation = "hdfs:///tmp/sylph/spark/savepoints/";

    public StructuredStreamingSqlAnalyse(SparkSession sparkSession, boolean isCompile)
    {
        this.sparkSession = sparkSession;
        this.isCompile = isCompile;
        this.sparkBean = binder -> {
            binder.bind(SparkSession.class, sparkSession);
        };
    }

    @Override
    public void finish()
    {
    }

    @Override
    public void createStreamAsSelect(CreateStreamAsSelect statement)
            throws Exception
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
            StructType tableSparkType,
            Optional<WaterMark> optionalWaterMark,
            String className)
            throws ClassNotFoundException
    {
        IocFactory iocFactory = IocFactory.create(sparkBean, binder -> binder.bind(TableContext.class).byInstance(sourceContext));
        StructuredNodeLoader loader = new StructuredNodeLoader(iocFactory, isCompile);

        checkState(!optionalWaterMark.isPresent(), "spark streaming not support waterMark");
        Source<Dataset<Row>> source = loader.createSource(Class.forName(className), sourceContext.withConfig());

        source.createSource().createOrReplaceTempView(sourceContext.getTableName());
    }

    public void createSinkTable(TableContext sinkContext, StructType tableSparkType, String className)
            throws ClassNotFoundException
    {
        IocFactory iocFactory = IocFactory.create(sparkBean, binder -> binder.bind(TableContext.class, sinkContext));
        StructuredNodeLoader loader = new StructuredNodeLoader(iocFactory, isCompile);
        Sink<Dataset<Row>> sink = loader.createSink(Class.forName(className), sinkContext.withConfig());
        sinks.put(sinkContext.getTableName(), sink);
    }

    @Override
    public void createFunction(CreateFunction createFunction)
            throws Exception
    {
        //todo: need byte code
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

        sparkSession.udf().registerJava(functionName, functionClass.getName(), returnType);
    }

    @Override
    public void insertInto(InsertInto insert)
            throws Exception
    {
        String tableName = insert.getTableName();
        String query = insert.getSelectQuery().getQuery();
        Dataset<Row> df = sparkSession.sql(query);
        Sink<Dataset<Row>> op = sinks.get(tableName);
        if (op == null) {
            throw new IllegalStateException("table " + tableName + " not found");
        }
        op.run(df);  //.apply(df);
    }

    @Override
    public void selectQuery(SelectQuery statement)
            throws Exception
    {
        Dataset<Row> df = sparkSession.sql(statement.getQuery());
        DataStreamWriter<Row> writer = df.writeStream()
                .foreach(new ConsoleWriter())
                .trigger(Trigger.Continuous("90 seconds"))
                //.option("checkpointLocation", checkpointLocation)
                .outputMode(OutputMode.Append());
        if (!isCompile) {
            writer.start();
        }
    }

    private static class ConsoleWriter
            extends ForeachWriter<Row>
    {
        @Override
        public boolean open(long partitionId, long epochId)
        {
            return true;
        }

        @Override
        public void process(Row value)
        {
            System.out.println(value.mkString(","));
        }

        @Override
        public void close(Throwable errorOrNull)
        {
        }
    }
}
