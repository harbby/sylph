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
package ideal.sylph.runner.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
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

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream)
    {
        outPutStream.apply(dataStream); //active driver sink
        return null;
    }
}
