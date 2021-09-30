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
package com.github.harbby.sylph.runner.flink.runtime;

import com.github.harbby.sylph.api.Record;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class FlinkRecord
        implements Record
{
    private final Row row;
    private final TypeInformation<Row> typeInformation;

    public FlinkRecord(Row row, TypeInformation<Row> typeInformation)
    {
        this.row = row;
        this.typeInformation = typeInformation;
    }

    public Row get()
    {
        return this.row;
    }

    @Override
    public String mkString(String seq)
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < row.getArity(); i++) {
            stringBuilder.append(seq).append(row.getField(i));
        }
        return stringBuilder.substring(1);
    }

    @Override
    public <T> T getAs(String key)
    {
        if (typeInformation instanceof RowTypeInfo) {
            int index = ((RowTypeInfo) typeInformation).getFieldIndex(key);
            return getAs(index);
        }
        else {
            throw new IllegalStateException("typeInformation not is RowTypeInfo");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAs(int i)
    {
        return (T) row.getField(i);
    }

    @Override
    public int size()
    {
        return row.getArity();
    }

    @Override
    public String toString()
    {
        return row.toString();
    }

    public static Row parserRow(Record record)
    {
        if (record instanceof FlinkRecord) {
            return ((FlinkRecord) record).get();
        }
        else if (record instanceof DefaultRecord) {
            return org.apache.flink.types.Row.of(((DefaultRecord) record).getValues());
        }
        else {
            throw new UnsupportedOperationException("Not Unsupported row type: " + record.getClass());
        }
    }
}
