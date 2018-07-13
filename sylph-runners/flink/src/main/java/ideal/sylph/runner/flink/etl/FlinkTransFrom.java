package ideal.sylph.runner.flink.etl;

import ideal.sylph.api.etl.RealTimeTransForm;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class FlinkTransFrom
        extends RichFlatMapFunction<Row, Row>
{
    private final RealTimeTransForm realTimeTransForm;
    private final TypeInformation<Row> typeInformation;

    public FlinkTransFrom(RealTimeTransForm realTimeTransForm, TypeInformation<Row> typeInformation)
    {
        this.realTimeTransForm = realTimeTransForm;
        this.typeInformation = typeInformation;
    }

    @Override
    public void close()
            throws Exception
    {
        realTimeTransForm.close(null);
        super.close();
    }

    /**
     * 注意flink只触发一次 这点和saprk不同
     */
    @Override
    public void open(Configuration parameters)
            throws Exception
    {
        realTimeTransForm.open(0, 0);
        super.open(parameters);
    }

    @Override
    public void flatMap(Row row, Collector<Row> collector)
            throws Exception
    {
        ideal.sylph.api.Row[] rows = realTimeTransForm.process(new FlinkRow(row, typeInformation));
        for (ideal.sylph.api.Row outRow : rows) {
            collector.collect(FlinkRow.parserRow(outRow));
        }
    }
}
