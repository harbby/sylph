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
package ideal.sylph.etl;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class Field
        implements Serializable
{
    private final String name;
    private final Type javaType;
    private final String extend;

    public Field(String name, Type javaType, String extend)
    {
        this.name = requireNonNull(name, "Field name must not null");
        this.javaType = requireNonNull(javaType, "Field type must not null");
        this.extend = extend;
    }

    public String getName()
    {
        return name;
    }

    public Type getJavaType()
    {
        return javaType;
    }

    public Optional<String> getExtend()
    {
        return Optional.of(extend);
    }

    public Class<?> getJavaTypeClass()
    {
        return typeToClass(javaType);
    }

    /**
     * Convert ParameterizedType or Class to a Class.
     */
    public static Class<?> typeToClass(Type t)
    {
        if (t instanceof Class) {
            return (Class<?>) t;
        }
        else if (t instanceof ParameterizedType) {
            return ((Class<?>) ((ParameterizedType) t).getRawType());
        }
        throw new IllegalArgumentException("Cannot convert type to class");
    }
}
