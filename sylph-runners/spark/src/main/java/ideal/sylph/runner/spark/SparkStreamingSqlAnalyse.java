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
import ideal.sylph.parser.antlr.tree.Statement;
import ideal.sylph.parser.antlr.tree.WaterMark;
import ideal.sylph.runner.spark.etl.sparkstreaming.StreamNodeLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

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
    private final JobBuilder builder = new JobBuilder();
    private final StreamingContext ssc;
    private final PipelinePluginManager pluginManager;
    private final Bean sparkBean;

    public SparkStreamingSqlAnalyse(StreamingContext ssc, PipelinePluginManager pluginManager)
    {
        this.ssc = ssc;
        this.pluginManager = pluginManager;
        this.sparkBean = binder -> {
            binder.bind(StreamingContext.class, ssc);
            binder.bind(JavaStreamingContext.class, new JavaStreamingContext(ssc));
        };
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
        StreamNodeLoader loader = new StreamNodeLoader(pluginManager, iocFactory);

        checkState(!optionalWaterMark.isPresent(), "spark streaming not support waterMark");
        UnaryOperator<DStream<Row>> source = loader.loadSource(driverClass, sourceContext.withConfig());
        builder.addSource(source, tableSparkType, sourceContext.getSourceTable());
    }

    public void createSinkTable(SinkContext sinkContext, StructType tableSparkType)
    {
        final String driverClass = (String) sinkContext.withConfig().get("type");
        IocFactory iocFactory = IocFactory.create(sparkBean, binder -> binder.bind(SinkContext.class, sinkContext));
        StreamNodeLoader loader = new StreamNodeLoader(pluginManager, iocFactory);

        UnaryOperator<Dataset<Row>> outputStream = dataSet -> {
            checkQueryAndTableSinkSchema(dataSet.schema(), tableSparkType, sinkContext.getSinkTable());
            return loader.loadRDDSink(driverClass, sinkContext.withConfig()).apply(dataSet);
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
            df.show();
        });
    }

    private static class JobBuilder
    {
        private final List<Consumer<SparkSession>> handlers = new ArrayList<>();
        private UnaryOperator<DStream<Row>> source;
        private StructType schema;
        private String sourceTableName;

        private final Map<String, UnaryOperator<Dataset<Row>>> sinks = new HashMap<>();

        public void addSource(UnaryOperator<DStream<Row>> source, StructType schema, String sourceTableName)
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
            DStream<Row> inputStream = source.apply(null);
            SqlUtil.registerStreamTable(inputStream, sourceTableName, schema, handlers);
        }
    }
}
