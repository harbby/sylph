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
package ideal.common.ioc;

import ideal.common.function.Creater;

import java.util.HashMap;
import java.util.Map;

interface Binds
{
    default <T> Creater<T> get(Class<T> type)
    {
        return getOrDefault(type, null);
    }

    <T> Creater<T> getOrDefault(Class<T> type, Creater<T> defaultValue);

    public <T> Map<Class<?>, Creater<?>> getAllBeans();

    static Builder builder()
    {
        return new Builder();
    }

    static class Builder
    {
        private final Map<Class<?>, Creater<?>> bindMapping = new HashMap<>();

        public <T> Builder bind(Class<T> type, Creater<? extends T> creater)
        {
            Creater oldCreater = bindMapping.get(type);
            if (oldCreater != null) {
                throw new InjectorException(" Unable to create IocFactory, see the following errors:\n" +
                        "A binding to " + type.toString() + " was already configured at " + oldCreater);
            }
            bindMapping.put(type, creater);
            return this;
        }

        <T> void bindUpdate(Class<T> type, Creater<? extends T> creater)
        {
            bindMapping.put(type, creater);
        }

        public Binds build()
        {
            return new Binds()
            {
                @SuppressWarnings("unchecked")
                @Override
                public <T> Creater<T> getOrDefault(Class<T> type, Creater<T> defaultValue)
                {
                    return (Creater<T>) bindMapping.getOrDefault(type, defaultValue);
                }

                @Override
                public Map<Class<?>, Creater<?>> getAllBeans()
                {
                    return bindMapping;
                }

                @Override
                public String toString()
                {
                    return bindMapping.toString();
                }
            };
        }
    }
}
