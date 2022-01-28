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
package com.github.harbby.sylph.main.service;

import com.github.harbby.gadtry.base.Try;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.spi.Module;
import com.github.harbby.gadtry.spi.ModuleLoader;
import com.github.harbby.gadtry.spi.SecurityClassLoader;
import com.github.harbby.gadtry.spi.ServiceLoad;
import com.github.harbby.gadtry.spi.VolatileClassLoader;
import com.github.harbby.sylph.api.Operator;
import com.github.harbby.sylph.api.Plugin;
import com.github.harbby.sylph.api.RealTimePipeline;
import com.github.harbby.sylph.main.server.ServerMainConfig;
import com.github.harbby.sylph.spi.OperatorInfo;
import com.github.harbby.sylph.spi.Runner;
import com.github.harbby.sylph.spi.job.JobEngine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.github.harbby.sylph.spi.OperatorInfo.analyzePluginInfo;

public class OperatorManager
{
    private static final Logger logger = LoggerFactory.getLogger(OperatorManager.class);
    private final ConcurrentMap<String, ModuleInfo> userExtPlugins = new ConcurrentHashMap<>();
    private final Map<Runner, List<OperatorInfo>> internalOperators = new HashMap<>();
    private ModuleLoader<Plugin> loader;
    private final File pluginDir;
    private final List<Runner> runnerList = new ArrayList<>();

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
                ImmutableList.of("com.github.harbby.sylph.api.", "com.github.harbby.gadtry.")); //raed only sylph-api deps
        this.loader = ModuleLoader.<Plugin>newScanner()
                .setPlugin(Plugin.class)
                .setScanDir(pluginDir)
                .setLoader(ServiceLoad::serviceLoad)
                .setClassLoaderFactory(urls -> new VolatileClassLoader(urls, securityClassLoader))
                .setLoadHandler(module -> {
                    logger.info("loading module {} find {} Operator", module.getName(), module.getPlugins().size());
                    ModuleInfo moduleInfo = new ModuleInfo(module, new ArrayList<>());
                    analyzeModulePlugins(moduleInfo);
                    ModuleInfo old = userExtPlugins.put(module.getName(), moduleInfo);
                    if (old != null) {
                        Try.of(old::close).onFailure(e -> logger.warn("free old module failed", e)).doTry();
                    }
                }).load();
    }

    void withRunners(List<Runner> runnerList)
    {
        this.runnerList.addAll(runnerList);
        //first add system internal operators
        runnerList.forEach(x -> internalOperators.put(x, x.getInternalOperator()));
    }

    private void analyzeModulePlugins(ModuleInfo module)
    {
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
                List<JobEngine> engines = runner.analyzePlugin(javaClass);
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

    public Set<OperatorInfo> getPluginsInfo()
    {
        Set<OperatorInfo> set = new HashSet<>();
        internalOperators.forEach((k, v) -> set.addAll(v));
        userExtPlugins.forEach((k, v) -> set.addAll(v.getConnectors()));
        return set;
    }

    public Map<String, OperatorInfo> getOperators(JobEngine engine)
    {
        String engineName = engine.getClass().getName();
        Map<String, OperatorInfo> map = new HashMap<>();
        this.getPluginsInfo().stream()
                .filter(x -> x.isRealTime() || x.getOwnerEngine().contains(engineName))
                .forEach(info -> {
                    String endWith = "\u0001" + info.getPipelineType();
                    map.put(info.getName() + endWith, info);
                    if (!info.getName().equals(info.getDriverClass())) {
                        map.put(info.getDriverClass() + endWith, info);
                    }
                });
        return map;
    }

    public synchronized void reload()
            throws IOException
    {
        logger.warn("reloading connectors...");
        loader.reload(userExtPlugins);
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
