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
import org.apache.flink.api.common.functions.RuntimeContext;
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
        super.open(parameters);
        RuntimeContext context = getRuntimeContext();

        // get parallelism id
        int partitionId = (context.getNumberOfParallelSubtasks() > 0) ?
                (context.getIndexOfThisSubtask() + 1) : 0;

        realTimeSink.open(partitionId, 0);
    }

    @Override
    public void close()
            throws Exception
    {
        realTimeSink.close(null);
        super.close();
    }
}
