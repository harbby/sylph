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
package ideal.sylph.plugins.kafka.flink.utils;

import ideal.sylph.etl.SourceContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSchema
        implements KeyedDeserializationSchema<Row>, SerializationSchema<Row>
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final RowTypeInfo rowTypeInfo;

    public JsonSchema(SourceContext context)
    {
        ideal.sylph.etl.Row.Schema schema = context.getSchema();
        TypeInformation<?>[] types = schema.getFieldTypes().stream().map(TypeExtractor::createTypeInfo).toArray(TypeInformation<?>[]::new);
        String[] names = schema.getFieldNames().toArray(new String[0]);
        this.rowTypeInfo = new RowTypeInfo(types, names);
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
            row.setField(i, map.get(names[i]));
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

    @Override
    public byte[] serialize(Row element) {
        StringBuffer seriMsg=new StringBuffer();
        for (int i=0;i<element.getArity();i++){
            seriMsg.append(element.getField(i)).append(",");
        }
        return seriMsg.toString().substring(0,seriMsg.toString().length() - 1).getBytes(StandardCharsets.UTF_8);
    }
}
