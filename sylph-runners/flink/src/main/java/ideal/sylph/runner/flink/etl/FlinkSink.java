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

import ideal.sylph.etl.api.RealTimeSink;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * RichSinkFunction or OutputFormat
 */
public final class FlinkSink
        implements OutputFormat<Row>
{
    private final RealTimeSink realTimeSink;
    private final TypeInformation<Row> typeInformation;

    public FlinkSink(RealTimeSink realTimeSink, TypeInformation<Row> typeInformation)
    {
        this.realTimeSink = requireNonNull(realTimeSink, "realTimeSink is null");
        this.typeInformation = requireNonNull(typeInformation, "typeInformation is null");
    }

    @Override
    public void configure(Configuration parameters)
    {
    }

    @Override
    public void open(int taskNumber, int numTasks)
            throws IOException
    {
        realTimeSink.open(taskNumber, numTasks);
    }

    @Override
    public void writeRecord(Row record)
            throws IOException
    {
        realTimeSink.process(new FlinkRow(record, typeInformation));
    }

    @Override
    public void close()
            throws IOException
    {
        realTimeSink.close(null);
    }
}
