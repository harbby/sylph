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

import java.util.HashMap;
import java.util.Map;

public interface Binds
{
    public <T> T get(Class<T> type);

    static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final Map<Class<?>, Object> map = new HashMap<>();

        public <T> Builder bind(Class<T> type, T value)
        {
            map.put(type, value);
            return this;
        }

        public Binds build()
        {
            return new Binds()
            {
                @Override
                public <T> T get(Class<T> type)
                {
                    @SuppressWarnings("unchecked")
                    T value = (T) map.get(type);
                    return value;
                }

                @Override
                public String toString()
                {
                    return map.toString();
                }
            };
        }
    }
}
