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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.harbby.gadtry.base.Lazys;
import com.github.harbby.gadtry.classloader.DirClassLoader;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.exception.StandardErrorCode;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobEngine;
import ideal.sylph.spi.job.JobEngineHandle;
import ideal.sylph.spi.job.JobStore;

import java.io.File;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.function.Supplier;

import static com.github.harbby.gadtry.base.Strings.isNotBlank;
import static com.google.common.base.Preconditions.checkState;
import static ideal.sylph.main.service.JobEngineManager.getClassLoaderDependJars;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class JobEngineImpl
        implements JobEngine
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final long startTime = System.currentTimeMillis();
    private final JobEngineHandle jobEngineHandle;
    private final ContainerFactory factory;

    private final String name;
    private final String description;

    JobEngineImpl(JobEngineHandle jobEngineHandle, ContainerFactory factory)
    {
        this.factory = requireNonNull(factory, "factory is null");
        this.jobEngineHandle = requireNonNull(jobEngineHandle, "jobEngineHandle is null");
        this.name = getAnnotation(jobEngineHandle, Name.class).value();
        this.description = getAnnotation(jobEngineHandle, Description.class).value();
        checkState(isNotBlank(name), "%s Missing @Name annotation",
                jobEngineHandle.getClass().getName());
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public long getCreateTime()
    {
        return startTime;
    }

    @Override
    public ContainerFactory getFactory()
    {
        return factory;
    }

    @Override
    public URLClassLoader getHandleClassLoader()
    {
        return (URLClassLoader) jobEngineHandle.getClass().getClassLoader();
    }

    @Override
    public Job compileJob(JobStore.DbJob dbJob, File jobWorkDir)
            throws Exception
    {
        JobEngine jobEngine = this;
        String jobName = dbJob.getJobName();
        JobConfig jobConfig = MAPPER.readValue(dbJob.getConfig(), jobEngineHandle.getConfigParser());

        try (DirClassLoader jobClassLoader = new DirClassLoader(null, jobEngine.getHandleClassLoader())) {
            jobClassLoader.addDir(jobWorkDir);

            Flow flow = jobEngineHandle.formFlow(dbJob.getQueryText().getBytes(UTF_8));
            jobEngineHandle.parserFlowDepends(flow)
                    .forEach(plugin -> jobClassLoader.addDir(plugin.getPluginFile()));

            Supplier<Serializable> jobDAG = Lazys.goLazy(() ->
                    {
                        try {
                            return jobEngineHandle.formJob(jobName, flow, jobConfig, jobClassLoader);
                        }
                        catch (Exception e) {
                            throw new SylphException(StandardErrorCode.JOB_BUILD_ERROR, e);
                        }
                    }
            );
            Collection<URL> dependFiles = getClassLoaderDependJars(jobClassLoader);
            return new Job(dbJob.getId(), jobName, jobWorkDir, dependFiles, jobClassLoader, jobDAG, jobConfig);
        }
    }

    private <T extends Annotation> T getAnnotation(JobEngineHandle jobActuator, Class<T> annotationClass)
    {
        T annotation = jobActuator.getClass().getAnnotation(annotationClass);
        return requireNonNull(annotation, "Missing annotation " + annotationClass);
    }
}
