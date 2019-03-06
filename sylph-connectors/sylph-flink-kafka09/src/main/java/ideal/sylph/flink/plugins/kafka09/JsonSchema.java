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
package ideal.sylph.flink.plugins.kafka09;

import com.google.gson.Gson;
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
import java.util.HashMap;
import java.util.Map;

public class JsonSchema
        implements SerializationSchema<Row>
{
    private   String[] names;

    public JsonSchema(SourceContext context)
    {
         ideal.sylph.etl.Row.Schema schema = context.getSchema();
         names = schema.getFieldNames().toArray(new String[0]);
    }
    @Override
    public byte[] serialize(Row element) {
        StringBuffer seriMsg=new StringBuffer();
        Gson gson = new Gson();
        Map<String, Object> map = new HashMap<>();
        for (int i=0;i<names.length;i++){
            map.put(names[i], element.getField(i));
        }
        String message = gson.toJson(map);
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
