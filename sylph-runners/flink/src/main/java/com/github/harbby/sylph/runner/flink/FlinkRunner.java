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
package com.github.harbby.sylph.runner.flink;

import com.github.harbby.gadtry.aop.AopGo;
import com.github.harbby.gadtry.base.Files;
import com.github.harbby.gadtry.base.Try;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.function.AutoClose;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.spi.DynamicClassLoader;
import com.github.harbby.sylph.runner.flink.engines.FlinkMainClassEngine;
import com.github.harbby.sylph.runner.flink.engines.FlinkStreamSqlEngine;
import com.github.harbby.sylph.runner.flink.plugins.TestSource;
import com.github.harbby.sylph.spi.JobClient;
import com.github.harbby.sylph.spi.OperatorInfo;
import com.github.harbby.sylph.spi.Runner;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobEngine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkArgument;
import static java.util.Objects.requireNonNull;

public class FlinkRunner
        implements Runner
{
    public static final String FLINK_DIST = "flink-dist";
    public static final String APPLICATION_TYPE = "SYLPH_SPARK";
    private final Set<JobEngine> engines = new HashSet<>();
    private final org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper mapper;

    private final List<URL> flinkLibJars;
    private final File flinkDistJar;
    private final File flinkConfDirectory;
    private final List<URL> frameworkJars;
    private final JobClient flinkJobClient;

    public FlinkRunner()
            throws FileNotFoundException, MalformedURLException
    {
        DynamicClassLoader frameworkClassLoader = (DynamicClassLoader) this.getClass().getClassLoader();
        this.frameworkJars = ImmutableList.copy(frameworkClassLoader.getURLs());

        String flinkHome = requireNonNull(System.getenv("FLINK_HOME"), "FLINK_HOME not setting");
        checkArgument(new File(flinkHome).exists(), "FLINK_HOME " + flinkHome + " not exists");
        this.flinkConfDirectory = new File(flinkHome, "conf");

        List<File> flinkLibJars = Files.listFiles(new File(flinkHome, "lib"), true);
        this.flinkDistJar = flinkLibJars.stream()
                .filter(f -> f.getName().startsWith(FLINK_DIST))
                .findFirst()
                .orElseThrow(() -> new FileNotFoundException("error not search flink-dist*.jar"));

        List<URL> libUrls = new ArrayList<>();
        for (File jarFile : flinkLibJars) {
            libUrls.add(jarFile.toURI().toURL());
        }
        this.flinkLibJars = ImmutableList.copy(libUrls);

        //load framework extend lib jars
        this.flinkLibJars.forEach(frameworkClassLoader::addJarFile);
        frameworkClassLoader.addDir(new File("hadoop-lib"));
        this.mapper = new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper();

        this.flinkJobClient = AopGo.proxy(JobClient.class)
                .byInstance(new FlinkJobClient(flinkDistJar, this.flinkLibJars, flinkConfDirectory))
                .aop(binder -> binder.doAround(point -> {
                    try (AutoClose ignored = Try.openThreadContextClassLoader(FlinkRunner.class.getClassLoader())) {
                        return point.proceed();
                    }
                }).allMethod())
                .build();
    }

    @Override
    public Set<JobEngine> getEngines()
    {
        return engines;
    }

    @Override
    public void initialize()
    {
        IocFactory injector = IocFactory.create(binder -> {
            binder.bind(FlinkMainClassEngine.class).withSingle();
            binder.bind(FlinkStreamSqlEngine.class).withSingle();
        });
        Stream.of(FlinkMainClassEngine.class, FlinkStreamSqlEngine.class)
                .map(injector::getInstance)
                .forEach(engines::add);
    }

    @Override
    public JobConfig analyzeConfig(String configString)
            throws IOException
    {
        return mapper.readValue(configString, FlinkJobConfig.class);
    }

    @Override
    public JobClient getJobSubmitter()
    {
        return flinkJobClient;
    }

    public List<OperatorInfo> getInternalOperator()
    {
        OperatorInfo operatorInfo = OperatorInfo.analyzePluginInfo(TestSource.class,
                engines.stream().map(x -> x.getClass().getName())
                        .collect(Collectors.toList()));
        return ImmutableList.of(operatorInfo);
    }

    @Override
    public List<URL> getFrameworkJars()
    {
        return frameworkJars;
    }

    @Override
    public List<URL> getExtendLibJars()
    {
        return flinkLibJars;
    }
}
