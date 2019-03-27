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

import ideal.sylph.etl.Field;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.lang.reflect.Type;

public class FlinkRow
        implements Row
{
    private org.apache.flink.types.Row row;
    private final TypeInformation<org.apache.flink.types.Row> typeInformation;

    public FlinkRow(org.apache.flink.types.Row row, TypeInformation<org.apache.flink.types.Row> typeInformation)
    {
        this.row = row;
        this.typeInformation = typeInformation;
    }

    public org.apache.flink.types.Row get()
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
            return (T) row.getField(index);
        }
        else {
            throw new IllegalStateException("typeInformation not is RowTypeInfo");
        }
    }

    @Override
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

    public static org.apache.flink.types.Row parserRow(Row row)
    {
        if (row instanceof FlinkRow) {
            return ((FlinkRow) row).get();
        }
        else if (row instanceof DefaultRow) {
            return org.apache.flink.types.Row.of(((DefaultRow) row).getValues());
        }
        else {
            throw new UnsupportedOperationException("Not Unsupported row type: " + row.getClass());
        }
    }

    public static RowTypeInfo parserRowType(Schema schema)
    {
        String[] fieldNames = schema.getFields().stream().map(Field::getName).toArray(String[]::new);
        return new RowTypeInfo(schema.getFields().stream().map(field -> {
            Type javaType = field.getJavaType();
            return parserType(javaType);
        }).toArray(TypeInformation[]::new), fieldNames);
    }

    private static TypeInformation<?> parserType(Type javaType)
    {
        return TypeExtractor.createTypeInfo(javaType);
    }
}
