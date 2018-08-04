package ideal.sylph.runner.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

public class SylphTableSink
        implements TableSink<Row>, AppendStreamTableSink<Row>
{
    private final RowTypeInfo rowTypeInfo;
    private final UnaryOperator<DataStream<Row>> outPutStream;

    public SylphTableSink(final RowTypeInfo rowTypeInfo, UnaryOperator<DataStream<Row>> outPutStream)
    {
        this.rowTypeInfo = requireNonNull(rowTypeInfo, "rowTypeInfo is null");
        this.outPutStream = requireNonNull(outPutStream, "outPutStream is null");
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream)
    {
        outPutStream.apply(dataStream); //active driver sink
    }

    @Override
    public TypeInformation<Row> getOutputType()
    {
        return rowTypeInfo;
    }

    @Override
    public String[] getFieldNames()
    {
        return rowTypeInfo.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes()
    {
        return rowTypeInfo.getFieldTypes();
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes)
    {
        return this;
    }
}
