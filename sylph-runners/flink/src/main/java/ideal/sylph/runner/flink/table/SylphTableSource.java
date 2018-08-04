package ideal.sylph.runner.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

public class SylphTableSource
        implements TableSource<Row>, StreamTableSource<Row>
{
    private final RowTypeInfo rowTypeInfo;
    private final UnaryOperator<DataStream<Row>> inputStream;

    public SylphTableSource(final RowTypeInfo rowTypeInfo, UnaryOperator<DataStream<Row>> inputStream)
    {
        this.rowTypeInfo = requireNonNull(rowTypeInfo, "rowTypeInfo is null");
        this.inputStream = requireNonNull(inputStream, "outPutStream is null");
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv)
    {
        DataStream<Row> source = inputStream.apply(null);
        TypeInformation<Row> sourceType = source.getType();
        if (sourceType instanceof RowTypeInfo) {
            //TODO: select -> create table columns
//            source.map(inRow->{
//                return inRow.getField(0);
//            }).print();
        }
        return source;
    }

    @Override
    public TypeInformation<Row> getReturnType()
    {
        return rowTypeInfo;
    }

    @Override
    public TableSchema getTableSchema()
    {
        return TableSchema.fromTypeInfo(getReturnType());
    }

    @Override
    public String explainSource()
    {
        return TableConnectorUtil.generateRuntimeName(this.getClass(), getTableSchema().getColumnNames());
    }
}
