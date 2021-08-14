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

import com.github.harbby.gadtry.base.Closeables;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.easyspi.ModuleLoader;
import com.github.harbby.gadtry.ioc.Autowired;
import com.google.common.collect.ImmutableList;
import ideal.sylph.etl.Operator;
import ideal.sylph.main.server.ServerMainConfig;
import ideal.sylph.spi.OperatorMetaData;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobEngine;
import ideal.sylph.spi.job.JobStore;
import ideal.sylph.spi.model.OperatorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.Throwables.noCatch;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * JobEngineManager
 */
public class JobEngineManager
{
    private static final Logger logger = LoggerFactory.getLogger(JobEngineManager.class);
    private final Map<String, JobEngine> jobActuatorMap = new HashMap<>();
    private final ServerMainConfig config;
    private final OperatorManager operatorManager;
    private final RunnerContext runnerContext;

    private static final List<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("ideal.sylph.spi.")
            .add("com.github.harbby.gadtry")
            .add("ideal.sylph.annotation.")
            .add("ideal.sylph.etl.")  // etl api ?
            //-------------------------------------------------
            .add("com.fasterxml.jackson.annotation.")
            .add("com.fasterxml.jackson.")
            .add("org.openjdk.jol.")
            //----------test-------------
            //.add("com.google.inject.")
            .add("com.google.common.")
            .add("org.slf4j.")
            //.add("org.apache.log4j.")
            .build();

    @Autowired
    public JobEngineManager(OperatorManager operatorManager, ServerMainConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.operatorManager = operatorManager;
        this.runnerContext = engine -> {
            Set<OperatorInfo> operatorInfos = operatorManager.getPluginsInfo();
            List<OperatorInfo> filter = operatorInfos.stream()
                    .filter(x -> x.isRealTime() || x.getOwnerEngine().contains(engine.getClass().getName()))
                    .collect(Collectors.toList());
            return new OperatorMetaData(filter);
        };
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
        operatorManager.initializeRunners(runnerList);
    }

    private void initializeRunner(final Runner runner)
    {
        logger.info("Runner: {} starts loading {}", runner.getClass().getName(), Operator.class.getName());

        checkArgument(runner.getContainerFactory() != null, runner.getClass() + " getContainerFactory() return null");
        Throwables.noCatch(() -> runner.initialize(this.runnerContext));

        final ContainerFactory factory = noCatch(() -> runner.getContainerFactory().newInstance());
        runner.getEngines().forEach(jobActuatorHandle -> {
            JobEngine jobEngine = new JobEngineImpl(jobActuatorHandle, factory);
            String name = jobEngine.getName();

            //TODO: CHECK
            checkState(!jobActuatorMap.containsKey(name), "Multiple entries with same key: %s=%s and %s=%s", name,
                    jobActuatorMap.get(name),
                    name, jobEngine);

            jobActuatorMap.put(name, jobEngine);
        });
    }

    JobContainer createJobContainer(JobStore.DbJob dbJob, String runId, String runtimeType)
    {
        requireNonNull(runtimeType, "runtimeType is null");
        String jobType = requireNonNull(dbJob.getType(), "job Actuator Name is null " + dbJob.getJobName());
        JobEngine jobEngine = requireNonNull(jobActuatorMap.get(jobType), jobType + " not exists");

        Job job = noCatch(() -> jobEngine.compileJob(dbJob, new File(config.getJobWorkDir(), String.valueOf(dbJob.getId()))));
        ContainerFactory containerFactory = jobEngine.getFactory();
        switch (runtimeType) {
            case "yarn":
                try (Closeables ignored = Closeables.openThreadContextClassLoader(jobEngine.getHandleClassLoader())) {
                    return containerFactory.createYarnContainer(job, runId);
                }
            case "local":
                return containerFactory.createLocalContainer(job, runId);
            default:
                throw new IllegalArgumentException("this job.runtime.mode " + runtimeType + " have't support!");
        }
    }

    public List<String> getAllEngineNames()
    {
        return new ArrayList<>(jobActuatorMap.keySet());
    }

    public Job compileJob(JobStore.DbJob dbJob)
            throws Exception
    {
        JobEngine jobEngine = requireNonNull(jobActuatorMap.get(dbJob.getType()), dbJob.getType() + " not exists");
        return jobEngine.compileJob(dbJob, new File(config.getJobWorkDir(), String.valueOf(dbJob.getId())));
    }

    public static Collection<URL> getClassLoaderDependJars(final ClassLoader jobClassLoader)
    {
        ImmutableList.Builder<URL> builder = ImmutableList.<URL>builder();
        if (jobClassLoader instanceof URLClassLoader) {
            builder.add(((URLClassLoader) jobClassLoader).getURLs());

            final ClassLoader parentClassLoader = jobClassLoader.getParent();
            if (parentClassLoader instanceof URLClassLoader) {
                builder.add(((URLClassLoader) parentClassLoader).getURLs());
            }
        }
        return builder.build().stream().collect(Collectors.toMap(URL::getPath, v -> v, (x, y) -> y))  //distinct
                .values().stream().sorted(Comparator.comparing(URL::getPath))
                .collect(Collectors.toList());
    }
}
