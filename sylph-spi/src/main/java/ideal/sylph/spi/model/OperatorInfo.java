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
import com.google.common.collect.ImmutableSet;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.Operator;
import ideal.sylph.etl.OperatorType;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.api.RealTimePipeline;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.etl.api.RealTimeTransForm;
import ideal.sylph.etl.api.Sink;
import ideal.sylph.etl.api.Source;
import ideal.sylph.etl.api.TransForm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;
import static com.github.harbby.gadtry.base.Throwables.throwsThrowable;
import static ideal.sylph.spi.PluginConfigFactory.getPluginConfigDefaultValues;
import static java.util.Objects.requireNonNull;

public class OperatorInfo
        implements Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(OperatorInfo.class);

    private final boolean realTime;
    private final String[] names;
    private final String description;
    private final String version;
    private final String driverClass;
    //-------------
    private final OperatorType pipelineType;  //source transform or sink
    private File moduleFile = null;
    private final List<Map<String, Object>> pluginConfig; //Injected by the specific runner
    private final List<String> ownerEngine;

    private OperatorInfo(
            String[] names,
            String description,
            String version,
            boolean realTime,
            String driverClass,
            OperatorType pipelineType,
            List<Map<String, Object>> pluginConfig,
            List<String> ownerEngine)
    {
        this.names = requireNonNull(names, "names is null");
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

    public String[] getNames()
    {
        return names;
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
        return Objects.hash(realTime, names, description, version, driverClass, moduleFile, pipelineType, pluginConfig);
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
                && Arrays.equals(this.names, other.names)
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
                .add("names", names)
                .add("description", description)
                .add("version", version)
                .add("driverClass", driverClass)
                .add("moduleFile", moduleFile)
                .add("pipelineType", pipelineType)
                .add("pluginConfig", pluginConfig)
                .toString();
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
                throw throwsThrowable(e);
            }
        }

        return ImmutableList.of();
    }

    public static OperatorInfo analyzePluginInfo(Class<? extends Operator> javaClass, List<String> ownerEngine)
    {
        OperatorType pipelineType = parserDriverType(javaClass);
        boolean realTime = RealTimePipeline.class.isAssignableFrom(javaClass); //is realTime ?

        Name name = javaClass.getAnnotation(Name.class);
        String[] nameArr = ImmutableSet.<String>builder()
                .add(name == null ? javaClass.getName() : name.value())
                .build().toArray(new String[0]);

        String isRealTime = realTime ? "RealTime " + pipelineType.name() : pipelineType.name();
        logger.info("loading {} Pipeline Plugin:{} ,the name is {}", isRealTime, javaClass, nameArr);

        Description description = javaClass.getAnnotation(Description.class);
        Version version = javaClass.getAnnotation(Version.class);

        List<Map<String, Object>> pluginConfig = analyzeOperatorDefaultValue(javaClass);
        return new OperatorInfo(
                nameArr,
                description == null ? "" : description.value(),
                version == null ? "" : version.value(),
                realTime,
                javaClass.getName(),
                pipelineType,
                pluginConfig,
                ownerEngine
        );
    }

    private static OperatorType parserDriverType(Class<? extends Operator> javaClass)
    {
        if (Source.class.isAssignableFrom(javaClass)) {
            return OperatorType.source;
        }
        else if (TransForm.class.isAssignableFrom(javaClass) || RealTimeTransForm.class.isAssignableFrom(javaClass)) {
            return OperatorType.transform;
        }
        else if (Sink.class.isAssignableFrom(javaClass) || RealTimeSink.class.isAssignableFrom(javaClass)) {
            return OperatorType.sink;
        }
        else {
            throw new IllegalArgumentException("Unknown type " + javaClass.getName());
        }
    }
}
