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

import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobEngine;
import com.github.harbby.sylph.spi.job.JobParser;
import com.github.harbby.sylph.spi.job.MainClassJobParser;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * see: org.apache.spark.deploy.Client#createContainerLaunchContext
 * see: org.apache.spark.deploy.yarn.ApplicationMaster#startUserApplication
 */
@Name("SparkMainClass")
@Description("this is FlinkMainClassActuator Actuator")
public class SparkMainClassEngine
        implements JobEngine
{
    @Override
    public Serializable compileJob(JobParser jobParser, JobConfig jobConfig)
            throws Exception
    {
        MainClassJobParser mainClassJobParser = (MainClassJobParser) jobParser;
        Supplier<SparkSession> supplier = (Supplier<SparkSession> & Serializable) () -> {
            try {
                Class<?> mainClass = Class.forName(mainClassJobParser.getMainClass());
                Method main = mainClass.getMethod("main", String[].class);
                main.invoke(null, (Object) mainClassJobParser.getArgs());
                return SparkUtil.getSparkSession(false);
            }
            catch (Exception e) {
                throw new IllegalStateException("job compile failed", e);
            }
        };
        supplier.get();
        return (Serializable) supplier;
    }

    @Override
    public JobParser analyze(String flowBytes)
    {
        return new MainClassJobParser(flowBytes);
    }

    @Override
    public List<Class<?>> keywords()
    {
        return Collections.emptyList();
    }
}
