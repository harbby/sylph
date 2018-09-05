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

import ideal.sylph.etl.api.RealTimeTransForm;
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
        ideal.sylph.etl.Collector<ideal.sylph.etl.Row> rowCollector = new ideal.sylph.etl.Collector<ideal.sylph.etl.Row>()
        {
            @Override
            public void collect(ideal.sylph.etl.Row record)
            {
                collector.collect(FlinkRow.parserRow(record));
            }

            @Override
            public void close()
            {
                collector.close();
            }
        };
        realTimeTransForm.process(new FlinkRow(row, typeInformation), rowCollector);
    }
}
