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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

public class TestTableSource
        implements TableSource<Row>, StreamTableSource<Row>
{
    @Override
    public TypeInformation<Row> getReturnType()
    {
        TypeInformation[] fieldTypes = {Types.STRING(), Types.STRING(), Types.LONG()};
        String[] fieldNames = {"topic", "user_id", "time"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
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
        return TableConnectorUtil.generateRuntimeName(this.getClass(), getTableSchema().getFieldNames());
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv)
    {
        return execEnv.addSource(new TestDataSource()).map(x -> Row.of(x.f0, x.f1, x.f2))
                .returns(getReturnType());
    }
}
