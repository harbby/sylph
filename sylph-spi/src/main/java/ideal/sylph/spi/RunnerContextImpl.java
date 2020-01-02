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

import com.github.harbby.gadtry.classloader.DirClassLoader;
import ideal.sylph.etl.Operator;
import ideal.sylph.spi.model.ConnectorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.tree.ClassTypeSignature;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ideal.sylph.spi.model.ConnectorInfo.getConnectorDefaultConfig;

public class RunnerContextImpl
        implements RunnerContext
{
    private static final Logger logger = LoggerFactory.getLogger(RunnerContextImpl.class);
    private final Supplier<Set<ConnectorInfo>> findPlugins;

    public RunnerContextImpl(Supplier<Set<ConnectorInfo>> findPlugins)
    {
        this.findPlugins = findPlugins;
    }

    @Override
    public ConnectorStore createConnectorStore(Set<Class<?>> connectorGenerics, Class<? extends Runner> runnerClass)
    {
        Set<String> filterKeyword = connectorGenerics.stream().map(Class::getName).collect(Collectors.toSet());

        Map<File, List<ConnectorInfo>> moduleInfo = findPlugins.get().stream()
                .filter(it -> {
                    if (it.isRealTime()) {
                        return true;
                    }
                    if (it.getJavaGenerics().length == 0) {
                        return false;
                    }

                    ClassTypeSignature typeSignature = (ClassTypeSignature) it.getJavaGenerics()[0];
                    String typeName = typeSignature.getPath().get(0).getName();
                    return filterKeyword.contains(typeName);
                }).collect(Collectors.groupingBy(ConnectorInfo::getPluginFile));

        Set<ConnectorInfo> plugins = moduleInfo.entrySet().stream()
                .flatMap(it -> {
                    try (DirClassLoader classLoader = new DirClassLoader(runnerClass.getClassLoader())) {
                        classLoader.addDir(it.getKey());
                        for (ConnectorInfo info : it.getValue()) {
                            try {
                                Class<? extends Operator> plugin = classLoader.loadClass(info.getDriverClass()).asSubclass(Operator.class);
                                List<Map<String, Object>> config = getConnectorDefaultConfig(plugin);
                                info.setPluginConfig(config);
                            }
                            catch (Exception e) {
                                logger.warn("parser driver config failed,with {}/{}", info.getPluginFile(), info.getDriverClass(), e);
                            }
                        }
                    }
                    catch (IOException e) {
                        logger.error("Plugins {} access failed, no plugin details will be available", it.getKey(), e);
                    }
                    return it.getValue().stream();
                }).collect(Collectors.toSet());

        return new ConnectorStore(plugins);
    }
}
