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
package com.github.harbby.sylph.runner.spark;

import com.github.harbby.gadtry.aop.AopGo;
import com.github.harbby.gadtry.base.Files;
import com.github.harbby.gadtry.base.Try;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.function.AutoClose;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.spi.DynamicClassLoader;
import com.github.harbby.sylph.runner.spark.yarn.SparkJobClient;
import com.github.harbby.sylph.spi.JobClient;
import com.github.harbby.sylph.spi.Runner;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobEngine;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkArgument;
import static java.util.Objects.requireNonNull;

public class SparkRunner
        implements Runner
{
    public static final String APPLICATION_TYPE = "SYLPH_SPARK";
    private final com.fasterxml.jackson.databind.ObjectMapper mapper;
    private Set<JobEngine> engines;
    private final String sparkHome;
    private final List<URL> sparkLibJars;
    private final List<URL> frameworkJars;
    private final JobClient jobClient;

    public SparkRunner()
            throws MalformedURLException
    {
        this.sparkHome = requireNonNull(System.getenv("SPARK_HOME"), "SPARK_HOME not setting");
        checkArgument(new File(sparkHome).exists(), "SPARK_HOME " + sparkHome + " not exists");
        DynamicClassLoader frameworkClassLoader = (DynamicClassLoader) this.getClass().getClassLoader();
        this.frameworkJars = ImmutableList.copy(frameworkClassLoader.getURLs());

        List<URL> libUrls = new ArrayList<>();
        for (File jarFile : Files.listFiles(new File(sparkHome, "jars"), true)) {
            libUrls.add(jarFile.toURI().toURL());
        }
        this.sparkLibJars = ImmutableList.copy(libUrls);
        //load framework extend lib jars
        sparkLibJars.forEach(frameworkClassLoader::addJarFile);
        frameworkClassLoader.addDir(new File("hadoop-lib"));
        mapper = new com.fasterxml.jackson.databind.ObjectMapper();

        this.jobClient = AopGo.proxy(JobClient.class)
                .byInstance(new SparkJobClient(sparkHome))
                .aop(binder -> binder.doAround(point -> {
                    try (AutoClose ignored = Try.openThreadContextClassLoader(SparkRunner.class.getClassLoader())) {
                        return point.proceed();
                    }
                }).allMethod())
                .build();
    }

    @Override
    public void initialize()
    {
        IocFactory injector = IocFactory.create(
                binder -> {
                    binder.bind(SparkMainClassEngine.class).withSingle();
                    binder.bind(SparkStreamingSqlEngine.class).withSingle();
                    binder.bind(StructuredStreamingSqlEngine.class).withSingle();
                });

        this.engines = Stream.of(
                        SparkMainClassEngine.class,
                        SparkStreamingSqlEngine.class,
                        StructuredStreamingSqlEngine.class
                ).map(injector::getInstance)
                .collect(Collectors.toSet());
    }

    @Override
    public JobConfig analyzeConfig(String configString)
            throws IOException
    {
        return mapper.readValue(configString, SparkJobConfig.class);
    }

    @Override
    public JobClient getJobSubmitter()
    {
        return jobClient;
    }

    @Override
    public Set<JobEngine> getEngines()
    {
        return engines;
    }

    @Override
    public List<URL> getFrameworkJars()
    {
        return frameworkJars;
    }

    @Override
    public List<URL> getExtendLibJars()
    {
        return sparkLibJars;
    }
}
