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

import com.github.harbby.gadtry.classloader.Module;
import ideal.sylph.etl.Plugin;
import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.PipelinePluginLoader;
import ideal.sylph.main.service.RunnerManager;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.model.ConnectorInfo;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static ideal.sylph.spi.exception.StandardErrorCode.SYSTEM_ERROR;
import static ideal.sylph.spi.exception.StandardErrorCode.UNKNOWN_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class SylphContextImpl
        implements SylphContext
{
    private JobManager jobManager;
    private RunnerManager runnerManger;
    private PipelinePluginLoader pluginLoader;

    SylphContextImpl(JobManager jobManager, RunnerManager runnerManger, PipelinePluginLoader pluginLoader)
    {
        this.jobManager = requireNonNull(jobManager, "jobManager is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger is null");
        this.pluginLoader = requireNonNull(pluginLoader, "runnerManger is null");
    }

    @Override
    public void saveJob(@NotNull String jobId, @NotNull String flowString, String jobType, @NotNull String jobConfig)
            throws Exception
    {
        requireNonNull(jobId, "jobId is null");
        requireNonNull(flowString, "flowString is null");
        requireNonNull(jobConfig, "jobConfig is null");
        Job job = runnerManger.formJobWithFlow(jobId, flowString.getBytes(UTF_8), jobType, jobConfig);
        jobManager.saveJob(job);
    }

    @Override
    public void stopJob(@NotNull String jobId)
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
    public void startJob(@NotNull String jobId)
    {
        jobManager.startJob(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public void deleteJob(@NotNull String jobId)
    {
        try {
            jobManager.removeJob(requireNonNull(jobId, "jobId is null"));
        }
        catch (IOException e) {
            throw new SylphException(SYSTEM_ERROR, "drop job " + jobId + " is fail", e);
        }
    }

    @NotNull
    @Override
    public Collection<Job> getAllJobs()
    {
        return jobManager.listJobs();
    }

    @Override
    public Optional<Job> getJob(String jobId)
    {
        return jobManager.getJob(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public Optional<JobContainer> getJobContainer(@NotNull String jobId)
    {
        return jobManager.getJobContainer(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public Optional<JobContainer> getJobContainerWithRunId(@NotNull String runId)
    {
        return jobManager.getJobContainerWithRunId(requireNonNull(runId, "runId is null"));
    }

    @Override
    public Collection<JobActuator.ActuatorInfo> getAllActuatorsInfo()
    {
        return runnerManger.getAllActuatorsInfo();
    }

    @Override
    public List<ConnectorInfo> getAllConnectors()
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
    public List<ConnectorInfo> getEnginePlugins(String actuator)
    {
        return runnerManger.getEnginePlugins(actuator);
    }
}
