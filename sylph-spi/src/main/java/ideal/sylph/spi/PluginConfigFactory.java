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
package ideal.sylph.spi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.harbby.gadtry.collection.mutable.MutableMap;
import com.github.harbby.gadtry.memory.UnsafeHelper;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class PluginConfigFactory
{
    public static final PluginConfigFactory INSTANCE = new PluginConfigFactory();  //scala object
    private static final Logger logger = LoggerFactory.getLogger(NodeLoader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PluginConfigFactory() {}

    public <T extends PluginConfig> T createPluginConfig(Class<T> type, Map<String, Object> config)
            throws Exception
    {
        T pluginConfig = pluginConfigInstance(type);
        //--- inject map config
        injectConfig(pluginConfig, config);
        return pluginConfig;
    }

    public static <T extends PluginConfig> T pluginConfigInstance(Class<T> type)
            throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException
    {
        checkState(!Modifier.isAbstract(type.getModifiers()), "%s is Interface or Abstract, unable to inject", type);

        //Ignore the constructor in the configuration class
        try {
            Constructor<? extends T> pluginConfigConstructor = type.getDeclaredConstructor();
            logger.debug("find 'no parameter' constructor with [{}]", type);
            pluginConfigConstructor.setAccessible(true);
            return pluginConfigConstructor.newInstance();
        }
        catch (NoSuchMethodException e) {
            logger.warn("Not find 'no parameter' constructor, use javassist inject with [{}]", type);
            // copy proxyConfig field value to pluginConfig ...
            return UnsafeHelper.allocateInstance2(type);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends PluginConfig> void injectConfig(T pluginConfig, Map<String, Object> config)
            throws IllegalAccessException, NoSuchFieldException
    {
        Map<String, Object> otherConfig = new HashMap<>(config);
        otherConfig.remove("type");
        Class<?> typeClass = pluginConfig.getClass();
        for (Field field : typeClass.getDeclaredFields()) {
            Name name = field.getAnnotation(Name.class);
            if (name != null) {
                field.setAccessible(true);
                Object value = otherConfig.remove(name.value());
                if (value != null) {
                    field.set(pluginConfig, MAPPER.convertValue(value, field.getType()));
                }
                else if (field.get(pluginConfig) == null) {
                    // Unable to inject via config, and there is no default value
                    logger.info("[PluginConfig] {} field {}[{}] unable to inject ,and there is no default value, config only {}", typeClass, field.getName(), name.value(), config);
                }
            }
        }

        Field field = PluginConfig.class.getDeclaredField("otherConfig");
        field.setAccessible(true);
        ((Map<String, Object>) field.get(pluginConfig)).putAll(otherConfig);
        logger.info("inject pluginConfig Class [{}], outObj is {}", typeClass, pluginConfig);
    }

    public static List<Map<String, Object>> getPluginConfigDefaultValues(Class<? extends PluginConfig> configClass)
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        PluginConfig pluginConfig = pluginConfigInstance(configClass);
        List<Map<String, Object>> mapList = new ArrayList<>();
        for (Field field : configClass.getDeclaredFields()) {
            Name name = field.getAnnotation(Name.class);
            if (name == null) {
                continue;
            }

            Description description = field.getAnnotation(Description.class);
            field.setAccessible(true);
            Object defaultValue = field.get(pluginConfig);
            Map<String, Object> fieldConfig = MutableMap.of(
                    "key", name.value(),
                    "description", description == null ? "" : description.value(),
                    "default", defaultValue == null ? "" : defaultValue);
            mapList.add(fieldConfig);
        }
        return mapList;
    }
}
