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
package ideal.sylph.runner.flink.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SelectField
        implements Serializable
{
    private final String fieldName;
    private final TypeInformation<?> type;
    private final String tableName;
    private final boolean isBatchTableField;
    private final int fieldIndex;

    private SelectField(String fieldName, TypeInformation<?> type, String tableName, boolean isBatchTableField, int fieldIndex)
    {
        this.fieldName = fieldName;
        this.tableName = tableName;
        this.type = type;
        this.isBatchTableField = isBatchTableField;
        this.fieldIndex = fieldIndex;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public TypeInformation<?> getType()
    {
        return type;
    }

    public boolean isBatchTableField()
    {
        return isBatchTableField;
    }

    public int getFieldIndex()
    {
        return fieldIndex;
    }

    public static SelectField of(String fieldName, TypeInformation<?> type, String tableName, boolean batchTableField, int fieldIndex)
    {
        return new SelectField(fieldName, type, tableName, batchTableField, fieldIndex);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fieldName, type, tableName, isBatchTableField, fieldIndex);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SelectField o = (SelectField) obj;
        return Objects.equals(fieldName, o.fieldName) &&
                Objects.equals(type, o.type) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(fieldIndex, o.fieldIndex) &&
                Objects.equals(isBatchTableField, o.isBatchTableField);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fieldName", fieldName)
                .add("type", type)
                .add("tableName", tableName)
                .add("isBatchTableField", isBatchTableField)
                .add("fieldIndex", fieldIndex)
                .toString();
    }
}
