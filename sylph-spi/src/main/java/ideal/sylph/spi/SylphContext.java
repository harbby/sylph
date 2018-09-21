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
package ideal.sylph.spi;

import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.model.PipelinePluginManager;

import javax.validation.constraints.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface SylphContext
{
    void saveJob(@NotNull String jobId, @NotNull String flow, @NotNull Map jobConfig)
            throws Exception;

    void stopJob(@NotNull String jobId);

    void startJob(@NotNull String jobId);

    void deleteJob(@NotNull String jobId);

    @NotNull
    Collection<Job> getAllJobs();

    Optional<Job> getJob(String jobId);

    Optional<JobContainer> getJobContainer(@NotNull String jobId);

    Optional<JobContainer> getJobContainerWithRunId(@NotNull String jobId);

    /**
     * get all Actuator Names
     */
    Collection<JobActuator.ActuatorInfo> getAllActuatorsInfo();

    List<PipelinePluginManager.PipelinePluginInfo> getPlugins();

    List<PipelinePluginManager.PipelinePluginInfo> getPlugins(String actuator);
}
