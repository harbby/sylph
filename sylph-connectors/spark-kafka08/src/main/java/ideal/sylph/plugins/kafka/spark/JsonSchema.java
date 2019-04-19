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
package ideal.sylph.plugins.kafka.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import ideal.sylph.etl.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static ideal.sylph.runner.spark.SQLHepler.schemaToSparkType;
import static java.nio.charset.StandardCharsets.UTF_8;

public class JsonSchema
        implements Serializable
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final StructType rowTypeInfo;

    public JsonSchema(Schema schema)
    {
        this.rowTypeInfo = schemaToSparkType(schema);
    }

    public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
            throws IOException
    {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = MAPPER.readValue(message, Map.class);
        String[] names = rowTypeInfo.names();
        Object[] values = new Object[names.length];
        for (int i = 0; i < names.length; i++) {
            String key = names[i];
            switch (key) {
                case "_topic":
                    values[i] = topic;
                    continue;
                case "_message":
                    values[i] = new String(message, UTF_8);
                    continue;
                case "_key":
                    values[i] = new String(messageKey, UTF_8);
                    continue;
                case "_partition":
                    values[i] = partition;
                    continue;
                case "_offset":
                    values[i] = offset;
                    continue;
            }

            Object value = map.get(key);
            DataType type = rowTypeInfo.apply(i).dataType();

            if (type instanceof MapType && ((MapType) type).valueType() == DataTypes.StringType) {
                scala.collection.mutable.Map convertValue = new scala.collection.mutable.HashMap(); //必须是scala的map
                for (Map.Entry entry : ((Map<?, ?>) value).entrySet()) {
                    convertValue.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
                }
                values[i] = convertValue;
            }
            else if (value instanceof ArrayType) {
                //Class<?> aClass = type.getTypeClass();
                //values[i] = MAPPER.convertValue(value, aClass);
                //todo: Spark List to Array
                values[i] = value;
            }
            else if (type == DataTypes.LongType) {
                values[i] = ((Number) value).longValue();
            }
            else {
                values[i] = value;
            }
        }
        return new GenericRowWithSchema(values, rowTypeInfo);
    }

    public boolean isEndOfStream(Row nextElement)
    {
        return false;
    }

    public StructType getProducedType()
    {
        return rowTypeInfo;
    }
}
