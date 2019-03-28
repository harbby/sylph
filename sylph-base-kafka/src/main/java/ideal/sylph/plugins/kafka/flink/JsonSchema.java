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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Map;

import static ideal.sylph.runner.flink.actuator.StreamSqlUtil.schemaToRowTypeInfo;

public class JsonSchema
        implements KeyedDeserializationSchema<Row>
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final RowTypeInfo rowTypeInfo;

    public JsonSchema(Schema schema)
    {
        this.rowTypeInfo = schemaToRowTypeInfo(schema);
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
