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
package ideal.sylph.etl;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class Schema
        implements Serializable
{
    private final List<Field> fields;
    private final List<String> fieldNames;
    private final List<Type> types;

    private Schema(List<Field> fields)
    {
        this.fields = requireNonNull(fields, "fields must not null");
        this.fieldNames = fields.stream().map(Field::getName).collect(Collectors.toList());
        this.types = fields.stream().map(Field::getJavaType).collect(Collectors.toList());
    }

    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    public int getFieldIndex(String fieldName)
    {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldNames.get(i).equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    public List<Type> getFieldTypes()
    {
        return types;
    }

    public List<Field> getFields()
    {
        return fields;
    }

    public static SchemaBuilder newBuilder()
    {
        return new SchemaBuilder();
    }

    public static class SchemaBuilder
    {
        private final List<Field> fields = new ArrayList<>();

        public SchemaBuilder add(String name, Type javaType)
        {
            fields.add(new Field(name, javaType));
            return this;
        }

        public Schema build()
        {
            return new Schema(new ArrayList<>(fields));
        }
    }
}
