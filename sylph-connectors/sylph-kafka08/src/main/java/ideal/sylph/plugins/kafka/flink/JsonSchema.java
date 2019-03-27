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
package ideal.sylph.plugins.kafka.flink;

import ideal.sylph.etl.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class JsonSchema
        implements KeyedDeserializationSchema<Row>
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final RowTypeInfo rowTypeInfo;

    public JsonSchema(Schema schema)
    {
        this.rowTypeInfo = schemaToRowTypeInfo(schema);
    }

    public static RowTypeInfo schemaToRowTypeInfo(Schema schema)
    {
        TypeInformation<?>[] types = schema.getFieldTypes().stream().map(JsonSchema::getFlinkType)
                .toArray(TypeInformation<?>[]::new);
        String[] names = schema.getFieldNames().toArray(new String[0]);
        return new RowTypeInfo(types, names);
    }

    private static TypeInformation<?> getFlinkType(Type type)
    {
        if (type instanceof ParameterizedType && ((ParameterizedType) type).getRawType() == Map.class) {
            Type[] arguments = ((ParameterizedType) type).getActualTypeArguments();
            Type valueType = arguments[1];
            TypeInformation<?> valueInfo = getFlinkType(valueType);
            return new MapTypeInfo<>(TypeExtractor.createTypeInfo(arguments[0]), valueInfo);
        }
        else if (type instanceof ParameterizedType && ((ParameterizedType) type).getRawType() == List.class) {
            TypeInformation<?> typeInformation = getFlinkType(((ParameterizedType) type).getActualTypeArguments()[0]);
            if (typeInformation.isBasicType() && typeInformation != Types.STRING) {
                return Types.PRIMITIVE_ARRAY(typeInformation);
            }
            else {
                return Types.OBJECT_ARRAY(typeInformation);
            }
        }
        else {
            return TypeExtractor.createTypeInfo(type);
        }
    }

    @Override
    public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
            throws IOException
    {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = MAPPER.readValue(message, Map.class);
        String[] names = rowTypeInfo.getFieldNames();
        Row row = new Row(names.length);
        for (int i = 0; i < names.length; i++) {
            Object value = map.get(names[i]);
            Class<?> aClass = rowTypeInfo.getTypeAt(i).getTypeClass();
            if (aClass.isArray()) {
                row.setField(i, MAPPER.convertValue(value, aClass));
            }
            else {
                row.setField(i, value);
            }
        }
        return row;
    }

    @Override
    public boolean isEndOfStream(Row nextElement)
    {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType()
    {
        return rowTypeInfo;
    }
}
