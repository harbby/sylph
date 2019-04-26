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
package ideal.sylph.runner.flink.udf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * udtf
 * <p>
 * bug: Will cause problems with the join dimension table
 * Recommended UDFJson.class
 */
final class JsonParser
        extends TableFunction<Map<String, String>>
{
    private static final Logger logger = LoggerFactory.getLogger(JsonParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonParser() {}

    @Override
    public TypeInformation<Map<String, String>> getResultType()
    {
        //return Types.ROW(Types.STRING,Types.STRING);
        return Types.MAP(Types.STRING, Types.STRING);
    }

    /**
     * @return Map[string, json string or null]
     */
    @SuppressWarnings("unchecked")
    public void eval(final String jsonStr, final String... keys)
    {
        try {
            Map<String, Object> object = MAPPER.readValue(jsonStr, Map.class);
            Stream<String> keyStream = keys.length == 0 ? object.keySet().stream() : Stream.of(keys);

            Map<String, String> out = keyStream
                    .collect(Collectors.toMap(k -> k, v -> {
                        Object value = object.get(v);
                        return value instanceof String ? (String) value : value.toString();
                    }, (k1, k2) -> k1));
            collect(out);
        }
        catch (IOException e) {
            logger.error("parser json failed:{}", jsonStr, e);
        }
    }
}
