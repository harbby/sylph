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

import com.github.harbby.gadtry.easyspi.Module;
import ideal.sylph.etl.Plugin;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobStore;
import ideal.sylph.spi.model.JobInfo;
import ideal.sylph.spi.model.OperatorInfo;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface SylphContext
{
    void saveJob(JobStore.DbJob dbJob)
            throws Exception;

    void stopJob(int jobId);

    void startJob(int jobId);

    void deleteJob(int jobId);

    List<JobInfo> getAllJobs();

    JobInfo getJob(int jobId);

    Optional<JobContainer> getJobContainer(int jobId);

    Optional<JobContainer> getJobContainerWithRunId(String runId);

    /**
     * get all Actuator Names
     */
    List<String> getAllEngineNames();

    List<OperatorInfo> getEnginePlugins(String actuator);

    List<OperatorInfo> getAllConnectors();

    List<Module<Plugin>> getAllConnectorModules();

    void reload()
            throws IOException;

    void deleteModule(String moduleName)
            throws IOException;
}
