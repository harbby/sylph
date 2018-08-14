package ideal.sylph.spi.model;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import sun.reflect.generics.tree.TypeArgument;

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

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
            throws ClassNotFoundException
    {
        PipelinePluginInfo info = findPluginInfo(driverString).orElseThrow(() -> new ClassNotFoundException("no such driver class " + driverString));
        return Class.forName(info.getDriverClass());
    }

    default Optional<PipelinePluginInfo> findPluginInfo(String driverString)
    {
        ImmutableMap.Builder<String, PipelinePluginInfo> builder = ImmutableMap.builder();
        getAllPlugins().forEach(it -> {
            Stream.of(it.getNames()).forEach(name -> builder.put(name, it));
        });
        Map<String, PipelinePluginInfo> plugins = builder.build();
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

        public PipelinePluginInfo(
                String[] names,
                String description,
                String version,
                boolean realTime,
                String driverClass,
                TypeArgument[] javaGenerics,
                File pluginFile)
        {
            this.names = requireNonNull(names, "names is null");
            this.description = requireNonNull(description, "description is null");
            this.version = requireNonNull(version, "version is null");
            this.realTime = realTime;
            this.driverClass = requireNonNull(driverClass, "driverClass is null");
            this.javaGenerics = requireNonNull(javaGenerics, "javaGenerics is null");
            this.pluginFile = requireNonNull(pluginFile, "pluginFile is null");
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
    }
}
