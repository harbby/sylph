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

import com.github.harbby.gadtry.base.Lazys;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import ideal.sylph.etl.Field;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.function.Supplier;

public class JsonDeserializer
        implements Serializable
{
    private static final Configuration jsonConfig = Configuration.builder()
            .jsonProvider(new JacksonJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .options(Option.SUPPRESS_EXCEPTIONS)  //path 不存在时返回null
            .build();

    private Supplier<ReadContext> jsonContext;

    public void initNewMessage(byte[] message)
    {
        this.jsonContext = Lazys.goLazy(() -> JsonPath.using(jsonConfig)
                .parse(new ByteArrayInputStream(message)));
    }

    public Object deserialize(Field field)
    {
        String keyPath = field.getExtend().orElse(field.getName());
        if (!keyPath.startsWith("$")) {
            keyPath = "$." + keyPath;
        }
        Object value = jsonContext.get().read(keyPath);
        if (value == null) {
            return null;
        }
        else {
            return value;
        }
    }
}
