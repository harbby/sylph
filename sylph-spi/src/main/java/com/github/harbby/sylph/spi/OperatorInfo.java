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
package com.github.harbby.sylph.spi;

import com.github.harbby.gadtry.base.Files;
import com.github.harbby.sylph.api.Operator;
import com.github.harbby.sylph.api.PluginConfig;
import com.github.harbby.sylph.api.RealTimePipeline;
import com.github.harbby.sylph.api.RealTimeSink;
import com.github.harbby.sylph.api.Sink;
import com.github.harbby.sylph.api.Source;
import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.api.annotation.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;
import static com.github.harbby.gadtry.base.Throwables.throwThrowable;
import static com.github.harbby.sylph.spi.utils.PluginFactory.getPluginConfigDefaultValues;
import static java.util.Objects.requireNonNull;

public class OperatorInfo
{
    private static final Logger logger = LoggerFactory.getLogger(OperatorInfo.class);

    private final boolean realTime;
    private final String name;
    private final String description;
    private final String version;
    private final String driverClass;
    //-------------
    private final OperatorType pipelineType;  //source transform or sink
    private File moduleFile;
    private final List<Map<String, Object>> pluginConfig; //Injected by the specific runner
    private final List<String> ownerEngine;

    private OperatorInfo(
            String name,
            String description,
            String version,
            boolean realTime,
            String driverClass,
            OperatorType pipelineType,
            List<Map<String, Object>> pluginConfig,
            List<String> ownerEngine)
    {
        this.name = requireNonNull(name, "names is null");
        this.description = requireNonNull(description, "description is null");
        this.version = requireNonNull(version, "version is null");
        this.realTime = realTime;
        this.driverClass = requireNonNull(driverClass, "driverClass is null");
        this.pipelineType = requireNonNull(pipelineType, "pipelineType is null");
        this.pluginConfig = pluginConfig;
        this.ownerEngine = ownerEngine;
    }

    public String getDriverClass()
    {
        return driverClass;
    }

    public boolean isRealTime()
    {
        return realTime;
    }

    public String getName()
    {
        return name;
    }

    public Optional<File> getModuleFile()
    {
        return Optional.ofNullable(moduleFile);
    }

    public String getDescription()
    {
        return description;
    }

    public String getVersion()
    {
        return version;
    }

    public OperatorType getPipelineType()
    {
        return pipelineType;
    }

    public List<Map<String, Object>> getPluginConfig()
    {
        return pluginConfig;
    }

    public void setModuleFile(File moduleFile)
    {
        this.moduleFile = moduleFile;
    }

    public List<String> getOwnerEngine()
    {
        return ownerEngine;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(realTime, name, description, version, driverClass, moduleFile, pipelineType, pluginConfig);
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

        OperatorInfo other = (OperatorInfo) obj;
        return Objects.equals(this.realTime, other.realTime)
                && Objects.equals(this.name, other.name)
                && Objects.equals(this.description, other.description)
                && Objects.equals(this.version, other.version)
                && Objects.equals(this.driverClass, other.driverClass)
                && Objects.equals(this.moduleFile, other.moduleFile)
                && Objects.equals(this.pipelineType, other.pipelineType)
                && Objects.equals(this.pluginConfig, other.pluginConfig);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("realTime", realTime)
                .add("name", name)
                .add("description", description)
                .add("version", version)
                .add("driverClass", driverClass)
                .add("moduleFile", moduleFile)
                .add("pipelineType", pipelineType)
                .add("pluginConfig", pluginConfig)
                .toString();
    }

    public List<File> moduleFiles()
    {
        if (moduleFile == null) {
            return Collections.emptyList();
        }
        return Files.listFiles(moduleFile, true);
    }

    /**
     * "This method can only be called by the runner, otherwise it will report an error No classFound"
     */
    public static List<Map<String, Object>> analyzeOperatorDefaultValue(Class<? extends Operator> javaClass)
    {
        Constructor<?>[] constructors = javaClass.getConstructors();
        checkState(constructors.length == 1, "Operator " + javaClass + " must one constructor");
        Constructor<?> constructor = constructors[0];

        for (Class<?> argmentType : constructor.getParameterTypes()) {
            if (!PluginConfig.class.isAssignableFrom(argmentType)) {
                continue;
            }

            try {
                return getPluginConfigDefaultValues(argmentType.asSubclass(PluginConfig.class));
            }
            catch (Exception e) {
                throw throwThrowable(e);
            }
        }

        return Collections.emptyList();
    }

    public static OperatorInfo analyzePluginInfo(Class<? extends Operator> javaClass, List<String> ownerEngine)
    {
        OperatorType pipelineType = parserType(javaClass);
        boolean realTime = RealTimePipeline.class.isAssignableFrom(javaClass); //is realTime ?

        Name name0 = javaClass.getAnnotation(Name.class);
        String name = name0 == null ? javaClass.getName() : name0.value();

        String isRealTime = realTime ? "RealTime " + pipelineType.name() : pipelineType.name();
        logger.info("loading {} Pipeline Plugin:{} ,the name is {}", isRealTime, javaClass, name);

        Description description = javaClass.getAnnotation(Description.class);
        Version version = javaClass.getAnnotation(Version.class);

        List<Map<String, Object>> pluginConfig = analyzeOperatorDefaultValue(javaClass);
        return new OperatorInfo(
                name,
                description == null ? "" : description.value(),
                version == null ? "" : version.value(),
                realTime,
                javaClass.getName(),
                pipelineType,
                pluginConfig,
                ownerEngine);
    }

    private static OperatorType parserType(Class<? extends Operator> javaClass)
    {
        if (Source.class.isAssignableFrom(javaClass)) {
            return OperatorType.source;
        }
        else if (Sink.class.isAssignableFrom(javaClass) || RealTimeSink.class.isAssignableFrom(javaClass)) {
            return OperatorType.sink;
        }
        else {
            throw new IllegalArgumentException("Unknown type " + javaClass.getName());
        }
    }
}
