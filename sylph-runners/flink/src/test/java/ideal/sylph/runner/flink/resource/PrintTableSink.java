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
package ideal.sylph.runner.flink.resource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class PrintTableSink
        implements TableSink<Row>, AppendStreamTableSink<Row>
{
    public PrintTableSink() {}

    @Override
    public TypeInformation<Row> getOutputType()
    {
        RowTypeInfo rowTypeInfo = new RowTypeInfo(getFieldTypes(), getFieldNames());
        return rowTypeInfo;
    }

    @Override
    public String[] getFieldNames()
    {
        String[] fieldNames = {"product", "amount", "time"};
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes()
    {
        TypeInformation[] fieldTypes = {Types.STRING(), Types.STRING(), Types.LONG()};
        return fieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes)
    {
        return this;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream)
    {
        return dataStream.print();
    }
}
