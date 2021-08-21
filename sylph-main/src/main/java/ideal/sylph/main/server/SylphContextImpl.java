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
package ideal.sylph.main.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.harbby.gadtry.spi.Module;
import ideal.sylph.etl.Plugin;
import ideal.sylph.main.service.JobEngineManager;
import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.OperatorManager;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobStore;
import ideal.sylph.spi.model.JobInfo;
import ideal.sylph.spi.model.OperatorInfo;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static ideal.sylph.spi.exception.StandardErrorCode.SYSTEM_ERROR;
import static ideal.sylph.spi.exception.StandardErrorCode.UNKNOWN_ERROR;
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
    public void saveJob(JobStore.DbJob dbJob)
            throws Exception
    {
        requireNonNull(dbJob, "dbJob is null");
        jobManager.saveJob(dbJob);
    }

    @Override
    public void stopJob(@NotNull int jobId)
    {
        requireNonNull(jobId, "jobId is null");
        try {
            jobManager.stopJob(jobId);
        }
        catch (Exception e) {
            throw new SylphException(UNKNOWN_ERROR, e);
        }
    }

    @Override
    public void startJob(int jobId)
    {
        jobManager.startJob(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public void deleteJob(int jobId)
    {
        try {
            jobManager.removeJob(requireNonNull(jobId, "jobId is null"));
        }
        catch (Exception e) {
            throw new SylphException(SYSTEM_ERROR, "drop job " + jobId + " is fail", e);
        }
    }

    @NotNull
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
    public Optional<JobContainer> getJobContainer(int jobId)
    {
        return jobManager.getJobContainer(jobId);
    }

    @Override
    public Optional<JobContainer> getJobContainerWithRunId(@NotNull String runId)
    {
        return jobManager.getJobContainerWithRunId(requireNonNull(runId, "runId is null"));
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
