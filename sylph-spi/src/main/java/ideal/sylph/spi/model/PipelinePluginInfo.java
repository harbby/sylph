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

import com.github.harbby.gadtry.base.JavaTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.api.RealTimePipeline;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.etl.api.Sink;
import ideal.sylph.etl.api.Source;
import ideal.sylph.etl.api.TransForm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.tree.ClassTypeSignature;
import sun.reflect.generics.tree.SimpleClassTypeSignature;
import sun.reflect.generics.tree.TypeArgument;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;
import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static ideal.sylph.spi.PluginConfigFactory.getPluginConfigDefaultValues;
import static java.util.Objects.requireNonNull;

public class PipelinePluginInfo
        implements Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(PipelinePluginInfo.class);
    private static Class<? extends PipelinePlugin> javaClass;

    private final boolean realTime;
    private final String[] names;
    private final String description;
    private final String version;
    private final String driverClass;
    private final transient TypeArgument[] javaGenerics;
    //-------------
    private final File pluginFile;
    private final PipelinePlugin.PipelineType pipelineType;  //source transform or sink

    private List<Map<String, Object>> pluginConfig = Collections.emptyList(); //Injected by the specific runner

    private PipelinePluginInfo(
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

    public boolean isRealTime()
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

    public List<Map<String, Object>> getPluginConfig()
    {
        return pluginConfig;
    }

    public void setPluginConfig(List<Map<String, Object>> config)
    {
        this.pluginConfig = config;
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

    /**
     * "This method can only be called by the runner, otherwise it will report an error No classFound"
     */
    public static List<Map<String, Object>> parserPluginDefaultConfig(Class<? extends PipelinePlugin> javaClass)
    {
        Constructor<?>[] constructors = javaClass.getConstructors();
        checkState(constructors.length == 1, "PipelinePlugin " + javaClass + " must one constructor");
        Constructor<?> constructor = constructors[0];

        for (Class<?> argmentType : constructor.getParameterTypes()) {
            if (!PluginConfig.class.isAssignableFrom(argmentType)) {
                continue;
            }

            try {
                return getPluginConfigDefaultValues(argmentType.asSubclass(PluginConfig.class));
            }
            catch (Exception e) {
                throw throwsException(e);
            }
        }

        return ImmutableList.of();
    }

    public static PipelinePluginInfo getPluginInfo(
            File pluginFile,
            Class<? extends PipelinePlugin> javaClass)
    {
        PipelinePlugin.PipelineType pipelineType = parserDriverType(javaClass);
        boolean realTime = RealTimePipeline.class.isAssignableFrom(javaClass); //is realTime ?
        TypeArgument[] javaGenerics = realTime ? new TypeArgument[0] : getClassGenericInfo(javaClass, pipelineType);

        Name name = javaClass.getAnnotation(Name.class);
        String[] nameArr = ImmutableSet.<String>builder()
                .add(name == null ? javaClass.getName() : name.value())
                .build().toArray(new String[0]);

        String isRealTime = realTime ? "RealTime " + pipelineType.name() : pipelineType.name();
        logger.info("loading {} Pipeline Plugin:{} ,the name is {}", isRealTime, javaClass, nameArr);

        Description description = javaClass.getAnnotation(Description.class);
        Version version = javaClass.getAnnotation(Version.class);

        return new PipelinePluginInfo(
                nameArr,
                description == null ? "" : description.value(),
                version == null ? "" : version.value(),
                realTime,
                javaClass.getName(),
                javaGenerics,
                pluginFile,
                pipelineType
        );
    }

    private static PipelinePlugin.PipelineType parserDriverType(Class<? extends PipelinePlugin> javaClass)
    {
        PipelinePluginInfo.javaClass = javaClass;
        if (Source.class.isAssignableFrom(javaClass)) {
            return PipelinePlugin.PipelineType.source;
        }
        else if (TransForm.class.isAssignableFrom(javaClass) || RealTimeTransForm.class.isAssignableFrom(javaClass)) {
            return PipelinePlugin.PipelineType.transform;
        }
        else if (Sink.class.isAssignableFrom(javaClass) || RealTimeSink.class.isAssignableFrom(javaClass)) {
            return PipelinePlugin.PipelineType.sink;
        }
        else {
            throw new IllegalArgumentException("Unknown type " + javaClass.getName());
        }
    }

    private static TypeArgument[] getClassGenericInfo(Class<? extends PipelinePlugin> javaClass, PipelinePlugin.PipelineType pipelineType)
    {
        Map<String, TypeArgument[]> typesMap = JavaTypes.getClassGenericInfo(javaClass);
        String genericString = JavaTypes.getClassGenericString(javaClass);

        logger.info("--The {} is not RealTimePipeline--the Java generics is {} --", javaClass, genericString);
        TypeArgument[] types = typesMap.getOrDefault(pipelineType.getValue().getName(), new TypeArgument[0]);
        return types;
    }

    public static List<String> getTypeNames(TypeArgument[] types)
    {
        return Arrays.stream(types)
                .flatMap(x -> ((ClassTypeSignature) x).getPath().stream())
                .map(SimpleClassTypeSignature::getName).collect(Collectors.toList());
    }
}
