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
package ideal.sylph.runner.flink.etl;

import com.github.harbby.gadtry.base.JavaTypes;
import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.etl.api.Sink;
import ideal.sylph.etl.api.Source;
import ideal.sylph.etl.api.TransForm;
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.exception.SylphException;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkState;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static java.util.Objects.requireNonNull;

public final class FlinkNodeLoader
        implements NodeLoader<DataStream<Row>>
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkNodeLoader.class);
    private final ConnectorStore connectorStore;
    private final IocFactory iocFactory;

    public FlinkNodeLoader(ConnectorStore connectorStore, IocFactory iocFactory)
    {
        this.connectorStore = requireNonNull(connectorStore, "binds is null");
        this.iocFactory = requireNonNull(iocFactory, "iocFactory is null");
    }

    @Override
    public UnaryOperator<DataStream<Row>> loadSource(String driverStr, final Map<String, Object> config)
    {
        final Class<?> driverClass = connectorStore.getConnectorDriver(driverStr, PipelinePlugin.PipelineType.source);
        checkState(Source.class.isAssignableFrom(driverClass),
                "The Source driver must is Source.class, But your " + driverClass);
        checkDataStreamRow(Source.class, driverClass);

        @SuppressWarnings("unchecked") final Source<DataStream<Row>> source = (Source<DataStream<Row>>) getPluginInstance(driverClass, config);

        return (stream) -> {
            logger.info("source {} schema:{}", driverClass, source.getSource().getType());
            return source.getSource();
        };
    }

    private static void checkDataStreamRow(Class<?> pluginInterface, Class<?> driverClass)
    {
        Type streamRow = JavaTypes.make(DataStream.class, new Type[] {Row.class}, null);
        Type checkType = JavaTypes.make(pluginInterface, new Type[] {streamRow}, null);

        for (Type type : driverClass.getGenericInterfaces()) {
            if (checkType.equals(type)) {
                return;
            }
        }
        throw new IllegalStateException(driverClass + " not is " + checkType + " ,your Generic is " + Arrays.asList(driverClass.getGenericInterfaces()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public UnaryOperator<DataStream<Row>> loadSink(String driverStr, final Map<String, Object> config)
    {
        Class<?> driverClass = connectorStore.getConnectorDriver(driverStr, PipelinePlugin.PipelineType.sink);
        checkState(RealTimeSink.class.isAssignableFrom(driverClass) || Sink.class.isAssignableFrom(driverClass),
                "The Sink driver must is RealTimeSink.class or Sink.class, But your " + driverClass);
        if (Sink.class.isAssignableFrom(driverClass)) {
            checkDataStreamRow(Sink.class, driverClass);
        }
        final Object driver = getPluginInstance(driverClass, config);

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

        return (stream) -> {
            requireNonNull(stream, "Sink find input stream is null");
            sink.run(stream);
            return null;
        };
    }

    @Override
    public IocFactory getIocFactory()
    {
        return iocFactory;
    }

    /**
     * transform api
     **/
    @SuppressWarnings("unchecked")
    @Override
    public final UnaryOperator<DataStream<Row>> loadTransform(String driverStr, final Map<String, Object> config)
    {
        Class<?> driverClass = connectorStore.getConnectorDriver(driverStr, PipelinePlugin.PipelineType.transform);
        checkState(RealTimeTransForm.class.isAssignableFrom(driverClass) || TransForm.class.isAssignableFrom(driverClass),
                "driverStr must is RealTimeSink.class or Sink.class");
        if (TransForm.class.isAssignableFrom(driverClass)) {
            checkDataStreamRow(TransForm.class, driverClass);
        }
        final Object driver = getPluginInstance(driverClass, config);

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

        return (stream) -> {
            requireNonNull(stream, "Transform find input stream is null");
            DataStream<Row> dataStream = transform.transform(stream);
            logger.info("transfrom {} schema to:", driver, dataStream.getType());
            return dataStream;
        };
    }

    private static Sink<DataStream<Row>> loadRealTimeSink(RealTimeSink realTimeSink)
    {
        // or user stream.addSink(new FlinkSink(realTimeSink, stream.getType()));
        return (Sink<DataStream<Row>>) stream -> stream.addSink(new FlinkSink(realTimeSink, stream.getType())).name(realTimeSink.getClass().getName());
    }

    private static TransForm<DataStream<Row>> loadRealTimeTransForm(RealTimeTransForm realTimeTransForm)
    {
        return (TransForm<DataStream<Row>>) stream -> {
            final SingleOutputStreamOperator<Row> tmp = stream
                    .flatMap(new FlinkTransFrom(realTimeTransForm, stream.getType()));
            // schema必须要在driver上面指定
            Schema schema = realTimeTransForm.getSchema();
            if (schema != null) {
                RowTypeInfo outPutStreamType = FlinkRow.parserRowType(schema);
                return tmp.returns(outPutStreamType);
            }
            return tmp;
        };
    }
}
