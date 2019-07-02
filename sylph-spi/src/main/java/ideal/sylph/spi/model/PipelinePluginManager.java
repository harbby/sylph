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
package ideal.sylph.spi.model;

import com.github.harbby.gadtry.classloader.DirClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.spi.Runner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.tree.ClassTypeSignature;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static com.google.common.base.Preconditions.checkState;
import static ideal.sylph.spi.model.PipelinePluginInfo.parserPluginDefualtConfig;
import static java.util.Objects.requireNonNull;

public interface PipelinePluginManager
        extends Serializable
{
    Logger logger = LoggerFactory.getLogger(PipelinePluginManager.class);

    /**
     * use test
     */
    public static PipelinePluginManager getDefault()
    {
        return new PipelinePluginManager()
        {
            @Override
            public Class<?> loadPluginDriver(String driverOrName, PipelinePlugin.PipelineType pipelineType)
            {
                try {
                    return Class.forName(driverOrName);
                }
                catch (ClassNotFoundException e) {
                    throw throwsException(e);
                }
            }
        };
    }

    default Set<PipelinePluginInfo> getAllPlugins()
    {
        return ImmutableSet.of();
    }

    default Class<?> loadPluginDriver(String driverOrName, PipelinePlugin.PipelineType pipelineType)
    {
        try {
            PipelinePluginInfo info = findPluginInfo(requireNonNull(driverOrName, "driverOrName is null"), pipelineType)
                    .orElseThrow(() -> new ClassNotFoundException("pipelineType:" + pipelineType + " no such driver class: " + driverOrName));
            return Class.forName(info.getDriverClass());
        }
        catch (ClassNotFoundException e) {
            throw throwsException(e);
        }
    }

    default Optional<PipelinePluginInfo> findPluginInfo(String driverOrName, PipelinePlugin.PipelineType pipelineType)
    {
        ImmutableTable.Builder<String, String, PipelinePluginInfo> builder = ImmutableTable.builder();

        this.getAllPlugins().forEach(info ->
                ImmutableList.<String>builder().add(info.getNames())
                        .add(info.getDriverClass()).build()
                        .stream()
                        .distinct()
                        .forEach(name -> builder.put(name + info.getPipelineType(), name, info))
        );
        ImmutableTable<String, String, PipelinePluginInfo> plugins = builder.build();

        if (pipelineType == null) {
            Map<String, PipelinePluginInfo> infoMap = plugins.column(driverOrName);
            checkState(infoMap.size() <= 1, "Multiple choices appear, please enter `type` to query" + infoMap);
            return infoMap.values().stream().findFirst();
        }
        else {
            return Optional.ofNullable(plugins.get(driverOrName + pipelineType, driverOrName));
        }
    }

    public static Set<PipelinePluginInfo> filterRunnerPlugins(
            Set<PipelinePluginInfo> findPlugins,
            Set<String> keyword,
            Class<? extends Runner> runnerClass)
    {
        Set<PipelinePluginInfo> plugins = findPlugins.stream()
                .filter(it -> {
                    if (it.getRealTime()) {
                        return true;
                    }
                    if (it.getJavaGenerics().length == 0) {
                        return false;
                    }
                    ClassTypeSignature typeSignature = (ClassTypeSignature) it.getJavaGenerics()[0];
                    String typeName = typeSignature.getPath().get(0).getName();
                    return keyword.contains(typeName);
                })
                .collect(Collectors.groupingBy(PipelinePluginInfo::getPluginFile))
                .entrySet().stream()
                .flatMap(it -> {
                    try (DirClassLoader classLoader = new DirClassLoader(runnerClass.getClassLoader())) {
                        classLoader.addDir(it.getKey());
                        for (PipelinePluginInfo info : it.getValue()) {
                            try {
                                Class<? extends PipelinePlugin> plugin = classLoader.loadClass(info.getDriverClass()).asSubclass(PipelinePlugin.class);
                                List<Map> config = parserPluginDefualtConfig(plugin, classLoader);
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

        return plugins;
    }
}
