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

import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.etl.PluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.UnaryOperator;

public interface NodeLoader<R>
{
    Logger logger = LoggerFactory.getLogger(NodeLoader.class);

    public UnaryOperator<R> loadSource(String driverStr, final Map<String, Object> pluginConfig);

    public UnaryOperator<R> loadTransform(String driverStr, final Map<String, Object> pluginConfig);

    public UnaryOperator<R> loadSink(String driverStr, final Map<String, Object> pluginConfig);

    /**
     * This method will generate the instance object by injecting the PipeLine interface.
     */
    default <T> T getPluginInstance(Class<T> driver, Map<String, Object> config)
    {
        return getPluginInstance(driver, getIocFactory(), config);
    }

    static <T> T getPluginInstance(Class<T> driver, IocFactory iocFactory, Map<String, Object> config)
    {
        return iocFactory.getInstance(driver, (type) -> {
            if (PluginConfig.class.isAssignableFrom(type)) { //config injection
                return PluginConfigFactory.INSTANCE.createPluginConfig(type.asSubclass(PluginConfig.class), config);
            }
            //throw new IllegalArgumentException(String.format("Cannot find instance of parameter [%s], unable to inject, only [%s]", type));
            return null;
        });
    }

    public IocFactory getIocFactory();
}
