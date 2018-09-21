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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.etl.PluginConfig;
import sun.reflect.generics.tree.TypeArgument;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public interface PipelinePluginManager
        extends Serializable
{
    /**
     * use test
     */
    public static PipelinePluginManager getDefault()
    {
        return new PipelinePluginManager()
        {
            @Override
            public Class<?> loadPluginDriver(String driverString)
                    throws ClassNotFoundException
            {
                return Class.forName(driverString);
            }
        };
    }

    default Set<PipelinePluginInfo> getAllPlugins()
    {
        return ImmutableSet.of();
    }

    default Class<?> loadPluginDriver(String driverString)
            throws ClassNotFoundException
    {
        PipelinePluginInfo info = findPluginInfo(driverString).orElseThrow(() -> new ClassNotFoundException("no such driver class " + driverString));
        return Class.forName(info.getDriverClass());
    }

    default Optional<PipelinePluginInfo> findPluginInfo(String driverString)
    {
        Map<String, PipelinePluginInfo> plugins = new HashMap<>();
        this.getAllPlugins().forEach(it -> {
            Stream.of(it.getNames()).forEach(name -> plugins.put(name, it));
            plugins.put(it.getDriverClass(), it);
        });
        return Optional.ofNullable(plugins.get(driverString));
    }

    public static class PipelinePluginInfo
            implements Serializable
    {
        private final boolean realTime;
        private final String[] names;
        private final String description;
        private final String version;
        private final String driverClass;
        private final transient TypeArgument[] javaGenerics;
        //-------------
        private final File pluginFile;
        private final PipelinePlugin.PipelineType pipelineType;  //source transform or sink
        private final List<Map> pluginConfig = Collections.emptyList(); //Injected by the specific runner

        public PipelinePluginInfo(
                String[] names,
                String description,
                String version,
                boolean realTime,
                String driverClass,
                TypeArgument[] javaGenerics,
                File pluginFile,
                PipelinePlugin.PipelineType pipelineType)
        {
            this.names = requireNonNull(names, "names is null");
            this.description = requireNonNull(description, "description is null");
            this.version = requireNonNull(version, "version is null");
            this.realTime = realTime;
            this.driverClass = requireNonNull(driverClass, "driverClass is null");
            this.javaGenerics = requireNonNull(javaGenerics, "javaGenerics is null");
            this.pluginFile = requireNonNull(pluginFile, "pluginFile is null");
            this.pipelineType = requireNonNull(pipelineType, "pipelineType is null");
        }

        public String getDriverClass()
        {
            return driverClass;
        }

        public boolean getRealTime()
        {
            return realTime;
        }

        public String[] getNames()
        {
            return names;
        }

        public File getPluginFile()
        {
            return pluginFile;
        }

        public TypeArgument[] getJavaGenerics()
        {
            return javaGenerics;
        }

        public String getDescription()
        {
            return description;
        }

        public String getVersion()
        {
            return version;
        }

        public PipelinePlugin.PipelineType getPipelineType()
        {
            return pipelineType;
        }

        public List<Map> getPluginConfig()
        {
            return pluginConfig;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(realTime, names, description, version, driverClass, javaGenerics, pluginFile, pipelineType, pluginConfig);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            PipelinePluginInfo other = (PipelinePluginInfo) obj;
            return Objects.equals(this.realTime, other.realTime)
                    && Arrays.equals(this.names, other.names)
                    && Objects.equals(this.description, other.description)
                    && Objects.equals(this.version, other.version)
                    && Objects.equals(this.driverClass, other.driverClass)
                    && Arrays.equals(this.javaGenerics, other.javaGenerics)
                    && Objects.equals(this.pluginFile, other.pluginFile)
                    && Objects.equals(this.pipelineType, other.pipelineType)
                    && Objects.equals(this.pluginConfig, other.pluginConfig);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("realTime", realTime)
                    .add("names", names)
                    .add("description", description)
                    .add("version", version)
                    .add("driverClass", driverClass)
                    .add("javaGenerics", javaGenerics)
                    .add("pluginFile", pluginFile)
                    .add("pipelineType", pipelineType)
                    .add("pluginConfig", pluginConfig)
                    .toString();
        }
    }

    static List<Map> parserDriverConfig(Class<? extends PipelinePlugin> javaClass)
    {
        for (Constructor<?> constructor : javaClass.getConstructors()) {
            for (Class<?> argmentType : constructor.getParameterTypes()) {
                if (PluginConfig.class.isAssignableFrom(argmentType)) {
                    return Arrays.stream(argmentType.getDeclaredFields())
                            .filter(field -> field.getAnnotation(Name.class) != null)
                            .map(field -> {
                                Name name = field.getAnnotation(Name.class);
                                Description description = field.getAnnotation(Description.class);
                                return ImmutableMap.of(
                                        "key", name.value(),
                                        "description", description == null ? "" : description.value()
                                );
                            }).collect(Collectors.toList());
                }
            }
        }
        return ImmutableList.of();
    }
}
