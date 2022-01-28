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
package com.github.harbby.sylph.main.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.harbby.gadtry.spi.Module;
import com.github.harbby.sylph.api.Plugin;
import com.github.harbby.sylph.main.SylphException;
import com.github.harbby.sylph.main.service.JobEngineManager;
import com.github.harbby.sylph.main.service.JobManager;
import com.github.harbby.sylph.main.service.OperatorManager;
import com.github.harbby.sylph.spi.OperatorInfo;
import com.github.harbby.sylph.spi.SylphContext;
import com.github.harbby.sylph.spi.dao.Job;
import com.github.harbby.sylph.spi.dao.JobInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SylphContextImpl
        implements SylphContext
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JobManager jobManager;
    private JobEngineManager runnerManger;
    private OperatorManager pluginLoader;

    SylphContextImpl(JobManager jobManager, JobEngineManager runnerManger, OperatorManager pluginLoader)
    {
        this.jobManager = requireNonNull(jobManager, "jobManager is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger is null");
        this.pluginLoader = requireNonNull(pluginLoader, "runnerManger is null");
    }

    @Override
    public void saveJob(Job job)
            throws Exception
    {
        requireNonNull(job, "dbJob is null");
        jobManager.saveJob(job);
    }

    @Override
    public void stopJob(int jobId)
    {
        requireNonNull(jobId, "jobId is null");
        try {
            jobManager.stopJob(jobId);
        }
        catch (Exception e) {
            throw new SylphException(e);
        }
    }

    @Override
    public void deployJob(int jobId)
            throws Exception
    {
        jobManager.deployJob(jobId);
    }

    @Override
    public void deleteJob(int jobId)
    {
        try {
            jobManager.removeJob(jobId);
        }
        catch (Exception e) {
            throw new SylphException("drop job " + jobId + " is fail", e);
        }
    }

    @Override
    public List<JobInfo> getAllJobs()
    {
        return requireNonNull(jobManager.listJobs());
    }

    @Override
    public JobInfo getJob(int jobId)
    {
        return jobManager.getJob(jobId);
    }

    @Override
    public String getJobWebUi(String jobIdOrRunId)
            throws Exception
    {
        return jobManager.getJobWebUi(jobIdOrRunId);
    }

    @Override
    public List<String> getAllEngineNames()
    {
        return runnerManger.getAllEngineNames();
    }

    @Override
    public List<OperatorInfo> getAllConnectors()
    {
        return new ArrayList<>(pluginLoader.getPluginsInfo());
    }

    @Override
    public List<Module<Plugin>> getAllConnectorModules()
    {
        return pluginLoader.getModules();
    }

    @Override
    public void reload()
            throws IOException
    {
        pluginLoader.reload();
    }

    @Override
    public void deleteModule(String moduleName)
            throws IOException
    {
        pluginLoader.deleteModule(moduleName);
    }

    @Override
    public List<OperatorInfo> getEnginePlugins(String actuator)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
