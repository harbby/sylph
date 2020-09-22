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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

public class SylphTableSink
        implements AppendStreamTableSink<Row>
{
    private final TableSchema tableSchema;
    private final UnaryOperator<DataStream<Row>> outPutStream;

    public SylphTableSink(final RowTypeInfo rowTypeInfo, UnaryOperator<DataStream<Row>> outPutStream)
    {
        requireNonNull(rowTypeInfo, "rowTypeInfo is null");
        this.outPutStream = requireNonNull(outPutStream, "outPutStream is null");

        TableSchema.Builder builder = TableSchema.builder();
        String[] names = rowTypeInfo.getFieldNames();
        for (int i = 0; i < rowTypeInfo.getArity(); i++) {
            DataType dataType = TypeConversions.fromLegacyInfoToDataType(rowTypeInfo.getTypeAt(i));
            builder.field(names[i], dataType);
        }
        this.tableSchema = builder.build();
    }

    @Override
    public DataType getConsumedDataType()
    {
        return tableSchema.toRowDataType();
    }

    @Override
    public TableSchema getTableSchema()
    {
        return tableSchema;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes)
    {
        return this;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream)
    {
        Supplier<DataStreamSink<?>> supplier = (Supplier<DataStreamSink<?>>) outPutStream.apply(dataStream); //active driver sink
        DataStreamSink<?> dataStreamSink = supplier.get();
        return dataStreamSink;
    }
}
