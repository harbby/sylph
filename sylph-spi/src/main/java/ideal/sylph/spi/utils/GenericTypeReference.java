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
package ideal.sylph.spi.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * demo:
 * Map<String, Object> config = MAPPER.readValue(json, new GenericTypeReference(Map.class, String.class, Object.class));
 */
public class GenericTypeReference
        extends TypeReference<Object>
{
    private ParameterizedType type;

    public GenericTypeReference(Class<?> rawType, Type... typeArguments)
    {
        //this.type = new MoreTypes.ParameterizedTypeImpl(null, rawType, typeArguments);
        this.type = ParameterizedTypeImpl.make(rawType, typeArguments, null);
    }

    @Override
    public java.lang.reflect.Type getType()
    {
        return this.type;
    }
}
