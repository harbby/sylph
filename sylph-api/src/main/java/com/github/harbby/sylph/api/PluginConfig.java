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
package com.github.harbby.sylph.api;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class PluginConfig
        implements Serializable
{
    private final Map<String, Object> otherConfig = new HashMap<>();

    @Override
    public String toString()
    {
        Map<String, Object> map = Arrays.stream(this.getClass().getDeclaredFields())
                .collect(Collectors.toMap(Field::getName, field -> {
                    field.setAccessible(true);
                    try {
                        Object value = field.get(this);
                        return value == null ? "" : value;
                    }
                    catch (IllegalAccessException e) {
                        throw new RuntimeException("PluginConfig " + this.getClass() + " Serializable failed", e);
                    }
                }));
        map.put("otherConfig", otherConfig);
        return map.toString();
    }

    public Map<String, Object> getOtherConfig()
    {
        return otherConfig;
    }
}
