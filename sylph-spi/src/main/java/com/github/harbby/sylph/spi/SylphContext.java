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

import com.github.harbby.gadtry.spi.Module;
import com.github.harbby.sylph.api.Plugin;
import com.github.harbby.sylph.spi.dao.Job;
import com.github.harbby.sylph.spi.dao.JobInfo;

import java.io.IOException;
import java.util.List;

public interface SylphContext
{
    void saveJob(Job job)
            throws Exception;

    void stopJob(int jobId);

    void deployJob(int jobId)
            throws Exception;

    void deleteJob(int jobId);

    List<JobInfo> getAllJobs();

    JobInfo getJob(int jobId);

    String getJobWebUi(String jobIdOrRunId)
            throws Exception;

    List<String> getAllEngineNames();

    List<OperatorInfo> getEnginePlugins(String actuator);

    List<OperatorInfo> getAllConnectors();

    List<Module<Plugin>> getAllConnectorModules();

    void reload()
            throws IOException;

    void deleteModule(String moduleName)
            throws IOException;
}
