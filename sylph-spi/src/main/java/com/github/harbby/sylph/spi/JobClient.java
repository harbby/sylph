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

import com.github.harbby.sylph.spi.dao.JobRunState;
import com.github.harbby.sylph.spi.job.DeployResponse;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobDag;

import java.util.Map;

public interface JobClient
{
    DeployResponse deployJobOnYarn(JobDag<?> job, JobConfig jobConfig)
            throws Exception;

    void closeJobOnYarn(String runId)
            throws Exception;

    public Map<Integer, JobRunState.Status> getAllJobStatus(Map<Integer, String> ids)
            throws Exception;
}
