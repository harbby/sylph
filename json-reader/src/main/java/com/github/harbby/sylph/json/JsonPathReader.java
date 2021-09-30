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
package com.github.harbby.sylph.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public abstract class JsonPathReader
        implements Serializable
{
    private static final Configuration jsonConfig = Configuration.builder()
            .jsonProvider(new JacksonJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.SUPPRESS_EXCEPTIONS)
            .options(Option.DEFAULT_PATH_LEAF_TO_NULL)  //path 不存在时返回null
            //.options(Option.SUPPRESS_EXCEPTIONS)  //path 不存在时返回null
            .build();
    private static final long serialVersionUID = 6208262176869833572L;

    private byte[] message;
    private ReadContext readContext;

    public void initNewMessage(byte[] message)
    {
        this.message = message;
        this.readContext = null;
    }

    public abstract void deserialize(KafkaRecord<byte[], byte[]> record, Object[] values);

    protected Object read(String keyPath)
    {
        if (readContext == null) {
            readContext = JsonPath.using(jsonConfig).parse(new String(message, StandardCharsets.UTF_8));
        }
        //Type type = field.getJavaType();
        //value = MAPPER.convertValue(value, typeFactory.constructType(type));
        return readContext.read(keyPath);
    }
}
