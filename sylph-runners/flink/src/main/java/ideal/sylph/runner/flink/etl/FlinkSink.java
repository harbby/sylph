package ideal.sylph.runner.flink.etl;

import ideal.sylph.api.etl.RealTimeSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

public final class FlinkSink
        extends RichSinkFunction<Row>
{
    private final RealTimeSink realTimeSink;
    private final TypeInformation<Row> typeInformation;

    public FlinkSink(RealTimeSink realTimeSink, TypeInformation<Row> typeInformation)
    {
        this.realTimeSink = realTimeSink;
        this.typeInformation = typeInformation;
    }

    @Override
    public void invoke(Row value, Context context)
            throws Exception
    {
        realTimeSink.process(new FlinkRow(value, typeInformation));
    }

    @Override
    public void open(Configuration parameters)
            throws Exception
    {
        realTimeSink.open(0, 0);
        super.open(parameters);
    }

    @Override
    public void close()
            throws Exception
    {
        realTimeSink.close(null);
        super.close();
    }
}
