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
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.spi.ModuleLoader;
import com.github.harbby.sylph.api.Operator;
import com.github.harbby.sylph.main.server.ServerMainConfig;
import com.github.harbby.sylph.spi.Runner;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * JobEngineManager
 */
public class JobEngineManager
{
    private static final Logger logger = LoggerFactory.getLogger(JobEngineManager.class);
    private final Map<String, JobEngineWrapper> jobActuatorMap = new HashMap<>();
    private final ServerMainConfig config;
    private final OperatorManager operatorManager;

    private static final List<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("com.github.harbby.sylph.spi.")
            .add("com.github.harbby.sylph.api.")
            .add("com.github.harbby.sylph.parser.")
            .add("com.github.harbby.gadtry.")
            .build();

    @Autowired
    public JobEngineManager(OperatorManager operatorManager, ServerMainConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.operatorManager = operatorManager;
    }

    public void loadRunners()
            throws IOException
    {
        List<Runner> runnerList = new ArrayList<>();
        ModuleLoader.<Runner>newScanner()
                .setPlugin(Runner.class)
                .setScanDir(new File("modules"))
                .accessSpiPackages(SPI_PACKAGES)
                .setLoadHandler(module -> {
                    logger.info("Found module dir directory {} Try to loading the runner", module.moduleFile());
                    List<Runner> plugins = module.getPlugins();
                    if (!plugins.isEmpty()) {
                        for (Runner runner : plugins) {
                            logger.info("Installing runner {} with dir{}", runner.getClass().getName(), runner);
                            initializeRunner(runner);
                            runnerList.add(runner);
                        }
                    }
                    else {
                        logger.warn("No service providers of type {}", Runner.class.getName());
                    }
                }).load();
        //begin analyze module plugin class
        operatorManager.withRunners(runnerList);
    }

    private void initializeRunner(final Runner runner)
    {
        logger.info("Runner: {} starts loading {}", runner.getClass().getName(), Operator.class.getName());
        Try.noCatch(runner::initialize);
        runner.getEngines().forEach(jobEngine -> {
            JobEngineWrapper wrapper = new JobEngineWrapper(jobEngine, runner);
            String name = wrapper.getName();
            JobEngineWrapper old = jobActuatorMap.put(name, wrapper);
            checkState(old == null, "Multiple job engine %s %s", name, old);
        });
    }

    public List<String> getAllEngineNames()
    {
        return new ArrayList<>(jobActuatorMap.keySet());
    }

    public JobEngineWrapper getJobEngine(String name)
    {
        return requireNonNull(jobActuatorMap.get(name), " not found JobEngine " + name);
    }
}
