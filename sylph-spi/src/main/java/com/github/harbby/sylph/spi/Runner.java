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
package com.github.harbby.sylph.spi;

import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.sylph.api.Operator;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobEngine;
import com.github.harbby.sylph.spi.utils.PluginFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface Runner
{
    public void initialize()
            throws Exception;

    public Set<JobEngine> getEngines();

    public JobClient getJobSubmitter();

    public default List<OperatorInfo> getInternalOperator()
    {
        return ImmutableList.empty();
    }

    public List<URL> getFrameworkJars();

    public List<URL> getExtendLibJars();

    public default List<URL> getClassloaderJars()
    {
        URLClassLoader urlClassLoader = (URLClassLoader) this.getClass().getClassLoader();
        return Arrays.asList(urlClassLoader.getURLs());
    }

    public JobConfig analyzeConfig(String configString)
            throws IOException;

    public default List<JobEngine> analyzePlugin(Class<? extends Operator> operatorClass)
    {
        return this.getEngines().stream()
                .filter(engine -> PluginFactory.analyzePlugin(operatorClass, engine))
                .collect(Collectors.toList());
    }
}
