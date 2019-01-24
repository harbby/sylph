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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.Checks.checkState;
import static java.util.Objects.requireNonNull;

public class SylphTableSource
        implements TableSource<Row>, StreamTableSource<Row>
{
    private final RowTypeInfo rowTypeInfo;
    private final DataStream<Row> inputStream;

    public SylphTableSource(final RowTypeInfo rowTypeInfo, DataStream<Row> inputStream)
    {
        this.rowTypeInfo = requireNonNull(rowTypeInfo, "rowTypeInfo is null");
        this.inputStream = requireNonNull(inputStream, "outPutStream is null");
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv)
    {
        DataStream<Row> source = inputStream;
        TypeInformation<Row> sourceType = source.getType();
        checkState(sourceType instanceof RowTypeInfo, "DataStream type not is RowTypeInfo");

        List<Integer> indexs = Arrays.stream(rowTypeInfo.getFieldNames())
                .map(((RowTypeInfo) sourceType)::getFieldIndex)
                .collect(Collectors.toList());
        return source.map(inRow -> Row.of(indexs.stream().map(index -> index == -1 ? null : inRow.getField(index)).toArray()))
                .returns(rowTypeInfo);
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
