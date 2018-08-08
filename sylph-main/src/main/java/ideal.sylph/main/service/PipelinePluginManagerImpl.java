package ideal.sylph.main.service;

import com.google.common.collect.ImmutableSet;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.etl.api.RealTimePipeline;
import ideal.sylph.spi.RunnerFactory;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.IncompleteAnnotationException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.reflect.Modifier.PRIVATE;
import static java.lang.reflect.Modifier.PUBLIC;

public class PipelinePluginManagerImpl
        implements PipelinePluginManager
{
    private final Logger logger;
    private final Set<PipelinePluginManager.PipelinePluginInfo> pluginsInfo;

    @Override
    public Set<PipelinePluginInfo> getAllPlugins()
    {
        return pluginsInfo;
    }

    @Override
    public Set<PipelinePluginInfo> getPublicPlugins()
    {
        return pluginsInfo.stream().filter(x -> x.getType() == PUBLIC).collect(Collectors.toSet());
    }

    @Override
    public Set<PipelinePluginInfo> getPrivatePlugins()
    {
        return pluginsInfo.stream().filter(x -> x.getType() == PRIVATE).collect(Collectors.toSet());
    }

    public PipelinePluginManagerImpl(Set<Class<?>> plugins, Class<? extends RunnerFactory> factoryClass)
    {
        logger = LoggerFactory.getLogger("daw");
        this.pluginsInfo = plugins.stream()
                .map(it -> {
                    try {
                        return parserDriver(factoryClass, (Class<? extends PipelinePlugin>) it);
                    }
                    catch (IncompleteAnnotationException e) {
                        throw new RuntimeException(it + " Annotation value not set, Please check scala code", e);
                    }
                }).collect(Collectors.toSet());
    }

    private PipelinePluginManager.PipelinePluginInfo parserDriver(Class<? extends RunnerFactory> factoryClass, Class<? extends PipelinePlugin> javaClass)
    {
        if (RealTimePipeline.class.isAssignableFrom(javaClass)) {
            logger.debug("this is RealTimePipeline: {}", javaClass);
            return getPluginInfo(factoryClass, javaClass, PUBLIC, new Type[0]);
        }
        Type type = javaClass.getGenericInterfaces()[0];
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type[] types = parameterizedType.getActualTypeArguments();
            logger.info("--The {} is not RealTimePipeline--the Java generics is {} --", javaClass, Arrays.asList(types));
            return getPluginInfo(factoryClass, javaClass, PRIVATE, types);
        }
        else {
            throw new RuntimeException("Unrecognized plugin:" + javaClass);
        }
    }

    private PipelinePluginManager.PipelinePluginInfo getPluginInfo(
            Class<? extends RunnerFactory> factoryClass,
            Class<? extends PipelinePlugin> javaClass,
            int type,
            Type[] javaGenerics)
    {
        Name[] names = javaClass.getAnnotationsByType(Name.class);

        String[] nameArr = ImmutableSet.<String>builder()
                .add(javaClass.getName())
                .add(Stream.of(names).map(Name::value).toArray(String[]::new))
                .build().toArray(new String[0]);

        String isRealTime = type == PUBLIC ? "RealTime" : "Not RealTime";
        logger.info("loading {} Pipeline Plugin:{} ,the name is {}", isRealTime, javaClass, nameArr);

        Description description = javaClass.getAnnotation(Description.class);
        Version version = javaClass.getAnnotation(Version.class);

        return new PipelinePluginManager.PipelinePluginInfo(
                nameArr,
                description == null ? "" : description.value(),
                version == null ? "" : version.value(),
                type,
                javaClass,
                factoryClass,
                javaGenerics
        );
    }
}
