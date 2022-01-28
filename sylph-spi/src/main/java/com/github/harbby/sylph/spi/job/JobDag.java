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
package com.github.harbby.sylph.spi.job;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobDag<T>
        implements Serializable
{
    private final Serializable jobGraph;
    private Map<String, String> jobConf = new HashMap<>();
    private List<URL> jars = new ArrayList<>();
    private final String name;

    public JobDag(Serializable jobGraph, String name)
    {
        this.jobGraph = jobGraph;
        this.name = name;
    }

    public void addConf(Map<String, String> confMap)
    {
        this.jobConf.putAll(confMap);
    }

    public void addJar(URL jarFile)
    {
        jars.add(jarFile);
    }

    public List<URL> getJars()
    {
        return new ArrayList<>(jars);
    }

    @SuppressWarnings("unchecked")
    public T getGraph()
    {
        return (T) jobGraph;
    }

    public String getName()
    {
        return name;
    }
}
