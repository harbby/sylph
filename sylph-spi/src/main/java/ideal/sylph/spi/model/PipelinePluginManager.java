package ideal.sylph.spi.model;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import ideal.sylph.spi.RunnerFactory;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public interface PipelinePluginManager
        extends Serializable
{
    default Set<PipelinePluginInfo> getAllPlugins()
    {
        return ImmutableSet.of();
    }

    /**
     * get runner public Pipeline Plugin
     * demo : RealTimeSink and RealTimeTransForm
     */
    default Set<PipelinePluginInfo> getPublicPlugins()
    {
        return ImmutableSet.of();
    }

    /**
     * get runner Private Pipeline Plugin
     */
    default Set<PipelinePluginInfo> getPrivatePlugins()
    {
        return ImmutableSet.of();
    }

    default Class<?> loadPluginDriver(String driverString)
    {
        ImmutableMap.Builder<String, Class> builder = ImmutableMap.builder();
        getAllPlugins().forEach(it -> {
            Stream.of(it.getNames()).forEach(name -> builder.put(name, it.getDriverClass()));
        });
        Map<String, Class> plugins = builder.build();
        return requireNonNull(plugins.get(driverString), "no such driver class " + driverString);
    }

    public static class PipelinePluginInfo
            implements Serializable
    {
        private final int type;
        private final String[] names;
        private final String description;
        private final String version;
        private final Class<?> driverClass;
        private final Class<? extends RunnerFactory> runnerClass;
        private final transient Type[] javaGenerics;

        public PipelinePluginInfo(
                String[] names,
                String description,
                String version,
                int type,
                Class<?> driverClass,
                Class<? extends RunnerFactory> runnerClass,
                Type[] javaGenerics)
        {
            this.names = requireNonNull(names, "names is null");
            this.description = requireNonNull(description, "description is null");
            this.version = requireNonNull(version, "version is null");
            this.type = type;
            this.driverClass = requireNonNull(driverClass, "driverClass is null");
            this.runnerClass = requireNonNull(runnerClass, "runnerClass is null");
            this.javaGenerics = requireNonNull(javaGenerics, "javaGenerics is null");
        }

        public Class<? extends RunnerFactory> getRunnerClass()
        {
            return runnerClass;
        }

        public Class<?> getDriverClass()
        {
            return driverClass;
        }

        public int getType()
        {
            return type;
        }

        public String[] getNames()
        {
            return names;
        }

        public String getDescription()
        {
            return description;
        }

        public String getVersion()
        {
            return version;
        }
    }
}
