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
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * udtf
 */
public final class JsonParser
        extends TableFunction<Row>
{
    private static final Logger logger = LoggerFactory.getLogger(JsonParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public JsonParser() {}

    private void transEval(final String jsonStr, final String... keys)
    {
        try {
            Map object = MAPPER.readValue(jsonStr, Map.class);
            Row row = new Row(keys.length);
            for (int i = 0; i < keys.length; i++) {
                Object value = object.get(keys[i]);
                row.setField(i, value == null ? null : value.toString());
            }
            collect(row);
        }
        catch (Exception e) {
            logger.error("parser json failed:{}", jsonStr, e);
        }
    }

    public void eval(final String str, final String keys)
    {
        this.transEval(str, keys.split(","));
    }
}
