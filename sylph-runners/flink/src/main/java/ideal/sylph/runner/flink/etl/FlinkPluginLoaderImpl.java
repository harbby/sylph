package ideal.sylph.runner.flink.etl;

import ideal.sylph.api.etl.RealTimeSink;
import ideal.sylph.api.etl.RealTimeTransForm;
import ideal.sylph.api.etl.Sink;
import ideal.sylph.api.etl.Source;
import ideal.sylph.api.etl.TransForm;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.exception.SylphException;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.UnaryOperator;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

public final class FlinkPluginLoaderImpl
        implements NodeLoader<StreamTableEnvironment, DataStream<Row>>
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkPluginLoaderImpl.class);

    @Override
    public UnaryOperator<DataStream<Row>> loadSource(final StreamTableEnvironment tableEnv, final Map<String, Object> config)
    {
        try {
            final ClassLoader classLoader = this.getClass().getClassLoader();
            final String driverStr = (String) config.get("driver");
            final Class<? extends Source> clazz = (Class<? extends Source>) classLoader.loadClass(driverStr);
            final Source<StreamTableEnvironment, DataStream<Row>> source = clazz.newInstance();

            source.driverInit(tableEnv, config);
            logger.info("source {} schema:{}", clazz, source.getSource().getType());
            return (stream) -> source.getSource();
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }
    }

    @Override
    public UnaryOperator<DataStream<Row>> loadSink(final Map<String, Object> config)
    {
        final Object driver;
        try {
            final String driverStr = (String) config.get("driver");
            driver = Class.forName(driverStr).newInstance();
        }
        catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }

        final Sink<DataStream<Row>> sink;
        if (driver instanceof RealTimeSink) {
            sink = loadRealTimeSink((RealTimeSink) driver);
        }
        else if (driver instanceof Sink) {
            sink = (Sink<DataStream<Row>>) driver;
        }
        else {
            throw new SylphException(JOB_BUILD_ERROR, "NOT SUPPORTED Sink:" + driver);
        }

        sink.driverInit(config); //传入参数
        return (stream) -> {
            sink.run(stream);
            return null;
        };
    }

    /**
     * transform api
     **/
    @Override
    public final UnaryOperator<DataStream<Row>> loadTransform(final Map<String, Object> config)
    {
        final Object driver;
        try {
            String driverStr = (String) config.get("driver");
            driver = Class.forName(driverStr).newInstance();
        }
        catch (IllegalAccessException | InstantiationException e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }
        catch (ClassNotFoundException e) {
            throw new SylphException(JOB_BUILD_ERROR, "no such driver class", e);
        }

        final TransForm<DataStream<Row>> transform;
        if (driver instanceof RealTimeTransForm) {
            transform = loadRealTimeTransForm((RealTimeTransForm) driver);
        }
        else if (driver instanceof TransForm) {
            transform = (TransForm<DataStream<Row>>) driver;
        }
        else {
            throw new SylphException(JOB_BUILD_ERROR, "NOT SUPPORTED TransForm:" + driver);
        }
        transform.driverInit(config);
        return (stream) -> {
            DataStream<Row> dataStream = transform.transform(stream);
            logger.info("transfrom {} schema to:", driver, dataStream.getType());
            return dataStream;
        };
    }

    private static Sink<DataStream<Row>> loadRealTimeSink(RealTimeSink realTimeSink)
    {
        return new Sink<DataStream<Row>>()
        {
            @Override
            public void run(DataStream<Row> stream)
            {
                stream.addSink(new FlinkSink(realTimeSink, stream.getType()));
            }

            @Override
            public void driverInit(Map<String, Object> optionMap)
            {
                realTimeSink.driverInit(optionMap);
            }
        };
    }

    private static TransForm<DataStream<Row>> loadRealTimeTransForm(RealTimeTransForm realTimeTransForm)
    {
        return new TransForm<DataStream<Row>>()
        {
            @Override
            public DataStream<Row> transform(DataStream<Row> stream)
            {
                final SingleOutputStreamOperator<Row> tmp = stream.flatMap(new FlinkTransFrom(realTimeTransForm, stream.getType()));
                // schema必须要在driver上面指定
                ideal.sylph.api.Row.Schema schema = realTimeTransForm.getRowSchema();
                if (schema != null) {
                    RowTypeInfo outPutStreamType = FlinkRow.parserRowType(schema);
                    return tmp.returns(outPutStreamType);
                }
                return tmp;
            }

            @Override
            public void driverInit(Map<String, Object> optionMap)
            {
                realTimeTransForm.driverInit(optionMap);
            }
        };
    }

    //  /**
    //    * udf
    //    **/
    //  object Local_timestamp extends ScalarFunction {
    //    //Timestamp
    //    def eval(time: Any): Timestamp = new Timestamp(time.toString.toLong) //System.currentTimeMillis()
    //  }
}
