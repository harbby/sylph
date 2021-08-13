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
package ideal.sylph.main.service;

import com.github.harbby.gadtry.base.Try;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.easyspi.Module;
import com.github.harbby.gadtry.easyspi.ModuleLoader;
import com.github.harbby.gadtry.easyspi.SecurityClassLoader;
import com.github.harbby.gadtry.easyspi.VolatileClassLoader;
import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.etl.Operator;
import ideal.sylph.etl.Plugin;
import ideal.sylph.etl.api.RealTimePipeline;
import ideal.sylph.main.server.ServerMainConfig;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.job.JobEngineHandle;
import ideal.sylph.spi.model.OperatorInfo;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static ideal.sylph.spi.model.OperatorInfo.analyzePluginInfo;

public class OperatorManager
{
    private static final Logger logger = LoggerFactory.getLogger(OperatorManager.class);
    private final ConcurrentMap<String, ModuleInfo> userExtPlugins = new ConcurrentHashMap<>();
    private final Map<Runner, List<OperatorInfo>> internalOperators = new HashMap<>();
    private ModuleLoader<Plugin> loader;
    private final File pluginDir;

    @Autowired
    public OperatorManager(ServerMainConfig config)
    {
        this.pluginDir = config.getPluginDir();
    }

    public void loadPlugins()
            throws Exception
    {
        //When scanning plugins, we only provide plugin.jar/!Plugin.class running environment class
        SecurityClassLoader securityClassLoader = new SecurityClassLoader(Plugin.class.getClassLoader(),
                ImmutableList.of("ideal.sylph.", "com.github.harbby.gadtry.")); //raed only sylph-api deps
        this.loader = ModuleLoader.<Plugin>newScanner()
                .setPlugin(Plugin.class)
                .setScanDir(pluginDir)
                .setLoader(OperatorManager::serviceLoad)
                .setClassLoaderFactory(urls -> new VolatileClassLoader(urls, securityClassLoader)
                {
                    @Override
                    protected void finalize()
                            throws Throwable
                    {
                        super.finalize();
                        logger.warn("Jvm gc free ClassLoader: {}", Arrays.toString(urls));
                    }
                })
                .setLoadHandler(module -> {
                    logger.info("loading module {} find {} Operator", module.getName(), module.getPlugins().size());
                    userExtPlugins.put(module.getName(), new ModuleInfo(module, new ArrayList<>()));
                }).load();
    }

    void analyzeModulePlugins(List<Runner> runnerList)
    {
        //first add system internal operators
        runnerList.forEach(x -> internalOperators.put(x, x.getInternalOperator()));

        for (ModuleInfo module : userExtPlugins.values()) {
            logger.info("analysing module {}", module.getName());
            List<OperatorInfo> infos = new ArrayList<>();
            List<Class<? extends Operator>> moduleOperators = module.getPlugins().stream().flatMap(x -> x.getConnectors().stream()).collect(Collectors.toList());
            for (Class<? extends Operator> javaClass : moduleOperators) {
                boolean realTime = RealTimePipeline.class.isAssignableFrom(javaClass);
                if (realTime) {
                    OperatorInfo operatorInfo = analyzePluginInfo(javaClass, Collections.emptyList());
                    operatorInfo.setModuleFile(module.moduleFile());
                    infos.add(operatorInfo);
                    continue;
                }

                for (Runner runner : runnerList) {
                    //use a magic scan
                    List<JobEngineHandle> engines = runner.analyzePlugin(javaClass);
                    if (!engines.isEmpty()) {
                        OperatorInfo operatorInfo = analyzePluginInfo(javaClass, engines.stream()
                                .map(x -> x.getClass().getName()).collect(Collectors.toList()));
                        operatorInfo.setModuleFile(module.moduleFile());
                        infos.add(operatorInfo);
                        logger.info("Operator {} support {}", javaClass, engines);
                        break;
                    }
                }
            }
            module.infos.addAll(infos);
        }
    }

    public Set<OperatorInfo> getPluginsInfo()
    {
        Set<OperatorInfo> set = new HashSet<>();
        internalOperators.forEach((k, v) -> set.addAll(v));
        userExtPlugins.forEach((k, v) -> set.addAll(v.getConnectors()));
        return set;
    }

    public void reload()
            throws IOException
    {
        logger.warn("reloading connectors...");
        loader.reload(userExtPlugins);
        logger.warn("reload connectors done.");
    }

    public void deleteModule(String moduleName)
            throws IOException
    {
        ModuleInfo moduleInfo = userExtPlugins.remove(moduleName);
        if (moduleInfo == null) {
            return;
        }
        moduleInfo.close();
        logger.info("removing module {}", moduleName);
        FileUtils.deleteDirectory(moduleInfo.module.moduleFile());
    }

    public List<Module<Plugin>> getModules()
    {
        return ImmutableList.copy(userExtPlugins.values());
    }

    private static final String PREFIX = "META-INF/services/";   // copy form ServiceLoader

    public static <T> Set<T> serviceLoad(Class<T> service, ClassLoader loader)
    {
        final String fullName = PREFIX + service.getName();

        Method parseLineMethod = Try.valueOf(() -> ServiceLoader.class.getDeclaredMethod("parseLine",
                        Class.class, URL.class, BufferedReader.class, int.class, List.class))
                .onSuccess(method -> method.setAccessible(true))
                .matchException(Exception.class, e -> {
                    throw new ServiceConfigurationError("Error find java.util.ServiceLoader.parse(Class.class, URL.class)");
                }).doTry();

        List<String> names = new ArrayList<>();
        try (InputStream in = loader.getResourceAsStream(fullName)) {
            if (in == null) {
                return Collections.emptySet();
            }
            BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            ServiceLoader<T> jdkServiceLoader = ServiceLoader.load(service); //don't foreach jdkServiceLoader
            Try.noCatch(() -> parseLineMethod.invoke(jdkServiceLoader, service, loader.getResource(fullName), r, 1, names));
        }
        catch (IOException e) {
            throw new ServiceConfigurationError(service.getName() + ": " + "Error reading configuration file", e);
        }
        return names.stream().map(cn -> {
            Class<?> c = null;
            try {
                c = Class.forName(cn, false, loader);
            }
            catch (ClassNotFoundException x) {
                throw new ServiceConfigurationError("Provider " + cn + " not found");
            }
            if (!service.isAssignableFrom(c)) {
                throw new ServiceConfigurationError("Provider " + cn + " not a subtype");
            }
            try {
                T p = service.cast(c.newInstance());
                return p;
            }
            catch (Throwable x) {
                throw new ServiceConfigurationError(service.getName() + ": " + "Provider " + cn + " could not be instantiated", x);
            }
        }).collect(Collectors.toSet());
    }

    private static class ModuleInfo
            implements Module<Plugin>
    {
        private final Module<Plugin> module;
        private final List<OperatorInfo> infos;

        public ModuleInfo(Module<Plugin> module, List<OperatorInfo> infos)
        {
            this.module = module;
            this.infos = infos;
        }

        public List<OperatorInfo> getConnectors()
        {
            return infos;
        }

        @Override
        public File moduleFile()
        {
            return module.moduleFile();
        }

        @Override
        public String getName()
        {
            return module.getName();
        }

        @Override
        public List<Plugin> getPlugins()
        {
            return module.getPlugins();
        }

        @Override
        public long getLoadTime()
        {
            return module.getLoadTime();
        }

        @Override
        public URLClassLoader getModuleClassLoader()
        {
            return module.getModuleClassLoader();
        }

        @Override
        public boolean modified()
        {
            return module.modified();
        }

        @Override
        public void close()
                throws IOException
        {
            logger.info("remove module {}, remove connector drivers {}", module.getName(), module.getPlugins()
                    .stream().flatMap(x -> x.getConnectors().stream()).collect(Collectors.toList()));
            module.close();
        }

        @Override
        protected void finalize()
                throws Throwable
        {
            super.finalize();
            logger.warn("Jvm gc free module {}", getName());
        }
    }
}
