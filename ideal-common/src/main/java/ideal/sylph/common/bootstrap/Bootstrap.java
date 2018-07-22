package ideal.sylph.common.bootstrap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.google.inject.spi.Message;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationLoader;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.configuration.ValidationErrorModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class Bootstrap
{
    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    private boolean strictConfig = false;
    private final List<Module> modules;
    private Map<String, String> optionalConfigurationProperties;
    private Map<String, String> requiredConfigurationProperties;
    private boolean requireExplicitBindings = true;

    public Bootstrap(Module... modules)
    {
        this(ImmutableList.copyOf(modules));
    }

    public Bootstrap(Iterable<? extends Module> modules)
    {
        this.modules = ImmutableList.copyOf(modules);
    }

    /**
     * 是否严格检查配置参数
     */
    public Bootstrap strictConfig()
    {
        this.strictConfig = true;
        return this;
    }

    public Bootstrap setOptionalConfigurationProperties(Map<String, String> optionalConfigurationProperties)
    {
        if (this.optionalConfigurationProperties == null) {
            this.optionalConfigurationProperties = new TreeMap<>();
        }

        this.optionalConfigurationProperties.putAll(optionalConfigurationProperties);
        return this;
    }

    public Bootstrap setRequiredConfigurationProperties(Map<String, String> requiredConfigurationProperties)
    {
        if (this.requiredConfigurationProperties == null) {
            this.requiredConfigurationProperties = new TreeMap<>();
        }

        this.requiredConfigurationProperties.putAll(requiredConfigurationProperties);
        return this;
    }

    /**
     * is Explicit Binding
     *
     * @param requireExplicitBindings true is Explicit
     */
    public Bootstrap requireExplicitBindings(boolean requireExplicitBindings)
    {
        this.requireExplicitBindings = requireExplicitBindings;
        return this;
    }

    public Injector initialize()
            throws Exception
    {
        logger.info("=========Bootstrap initialize...========");
        ConfigurationLoader loader = new ConfigurationLoader();

        Map<String, String> requiredProperties = new TreeMap<>();
        if (requiredConfigurationProperties == null) {
            String configFile = System.getProperty("config");
            requiredProperties.putAll(loader.loadPropertiesFrom(configFile));
        }
        //--------build: allProperties = required + optional + jvmProperties
        Map<String, String> allProperties = new TreeMap<>(requiredProperties);
        if (optionalConfigurationProperties != null) {
            allProperties.putAll(optionalConfigurationProperties);
        }
        allProperties.putAll(Maps.fromProperties(System.getProperties()));
        //-- create configurationFactory and registerConfig  and analysis config--
        ConfigurationFactory configurationFactory = new ConfigurationFactory(allProperties);
        configurationFactory.registerConfigurationClasses(this.modules);
        List<Message> messages = configurationFactory.validateRegisteredConfigurationProvider(); //对config进行装配
        TreeMap<String, String> unusedProperties = new TreeMap<>(requiredProperties);
        unusedProperties.keySet().removeAll(configurationFactory.getUsedProperties());
        //----
        ImmutableList.Builder<Module> moduleList = ImmutableList.builder();
        moduleList.add(new ConfigurationModule(configurationFactory));
        if (!messages.isEmpty()) {
            moduleList.add(new ValidationErrorModule(messages));
        }

        //Prevents Guice from constructing a Proxy when a circular dependency is found.
        moduleList.add(Binder::disableCircularProxies);
        if (this.requireExplicitBindings) {
            //Instructs the Injector that bindings must be listed in a Module in order to be injected.
            moduleList.add(Binder::requireExplicitBindings);
        }
        if (this.strictConfig) {
            moduleList.add((binder) -> {
                for (Map.Entry<String, String> unusedProperty : unusedProperties.entrySet()) {
                    binder.addError("Configuration property '%s' was not used", unusedProperty.getKey());
                }
            });
        }

        moduleList.addAll(this.modules);
        return Guice.createInjector(Stage.PRODUCTION, moduleList.build());
    }
}
