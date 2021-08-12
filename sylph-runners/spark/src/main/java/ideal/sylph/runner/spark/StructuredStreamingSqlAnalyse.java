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
import ideal.sylph.TableContext;
import ideal.sylph.etl.Schema;
import ideal.sylph.parser.antlr.tree.CreateFunction;
import ideal.sylph.parser.antlr.tree.CreateStreamAsSelect;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.parser.antlr.tree.InsertInto;
import ideal.sylph.parser.antlr.tree.SelectQuery;
import ideal.sylph.parser.antlr.tree.WaterMark;
import ideal.sylph.runner.spark.structured.StructuredNodeLoader;
import ideal.sylph.spi.OperatorMetaData;
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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static ideal.sylph.runner.spark.SQLHepler.checkQueryAndTableSinkSchema;
import static ideal.sylph.runner.spark.SQLHepler.getSparkType;
import static ideal.sylph.runner.spark.SQLHepler.getTableSchema;
import static ideal.sylph.runner.spark.SQLHepler.schemaToSparkType;

public class StructuredStreamingSqlAnalyse
        implements SqlAnalyse
{
    private final SparkSession sparkSession;
    private final OperatorMetaData operatorMetaData;
    private final Map<String, UnaryOperator<Dataset<Row>>> sinks = new HashMap<>();
    private final Bean sparkBean;
    private final boolean isCompile;

    //todo: use config
    private final String checkpointLocation = "hdfs:///tmp/sylph/spark/savepoints/";

    public StructuredStreamingSqlAnalyse(SparkSession sparkSession, OperatorMetaData operatorMetaData, boolean isCompile)
    {
        this.sparkSession = sparkSession;
        this.operatorMetaData = operatorMetaData;
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
    public void createTable(CreateTable createTable)
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
                createSourceTable(tableContext, tableSparkType, createTable.getWatermark());
                return;
            case SINK:
                createSinkTable(tableContext, tableSparkType);
                return;
            case BATCH:
                throw new UnsupportedOperationException("The SparkStreaming engine BATCH TABLE haven't support!");
            default:
                throw new IllegalArgumentException("this Connector " + connector + " haven't support!");
        }
    }

    public void createSourceTable(TableContext sourceContext, StructType tableSparkType, Optional<WaterMark> optionalWaterMark)
    {
        final String driverClass = sourceContext.getConnector();
        IocFactory iocFactory = IocFactory.create(sparkBean, binder -> binder.bind(TableContext.class).byInstance(sourceContext));
        StructuredNodeLoader loader = new StructuredNodeLoader(operatorMetaData, iocFactory);

        checkState(!optionalWaterMark.isPresent(), "spark streaming not support waterMark");
        UnaryOperator<Dataset<Row>> source = loader.loadSource(driverClass, sourceContext.withConfig());

        source.apply(null).createOrReplaceTempView(sourceContext.getTableName());
        //builder.addSource(source, tableSparkType, sourceContext.getSourceTable());
    }

    public void createSinkTable(TableContext sinkContext, StructType tableSparkType)
    {
        final String driverClass = sinkContext.getConnector();
        IocFactory iocFactory = IocFactory.create(sparkBean, binder -> binder.bind(TableContext.class, sinkContext));
        StructuredNodeLoader loader = new StructuredNodeLoader(operatorMetaData, iocFactory);

        UnaryOperator<Dataset<Row>> outputStream = dataSet -> {
            checkQueryAndTableSinkSchema(dataSet.schema(), tableSparkType, sinkContext.getTableName());
            DataStreamWriter<Row> writer = loader.loadSinkWithComplic(driverClass, sinkContext.withConfig()).apply(dataSet);
            if (!isCompile) {
                //UnsupportedOperationChecker.checkForContinuous();
                writer = writer.option("checkpointLocation", checkpointLocation);
                writer.start("");
            }
            return null;
        };
        sinks.put(sinkContext.getTableName(), outputStream);
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

        sparkSession.udf().registerJava(functionName, functionClass.getName(), returnType);
    }

    @Override
    public void insertInto(InsertInto insert)
            throws Exception
    {
        String tableName = insert.getTableName();
        String query = insert.getSelectQuery().getQuery();
        Dataset<Row> df = sparkSession.sql(query);
        UnaryOperator<Dataset<Row>> op = sinks.get(tableName);
        if (op == null) {
            throw new IllegalStateException("table " + tableName + " not found");
        }
        op.apply(df);  //.apply(df);
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
