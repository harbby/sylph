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

import com.github.harbby.gadtry.jvm.VmCallable;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobEngine;
import com.github.harbby.sylph.spi.job.JobParser;

import java.io.Serializable;

public class CompileTask
        implements VmCallable<Serializable>
{
    private final JobParser jobParser;
    private final JobEngine jobEngine;
    private final JobConfig jobConfig;

    public CompileTask(JobParser jobParser, JobEngine jobEngine, JobConfig jobConfig)
    {
        this.jobParser = jobParser;
        this.jobEngine = jobEngine;
        this.jobConfig = jobConfig;
    }

    @Override
    public Serializable call()
            throws Exception
    {
        return jobEngine.compileJob(jobParser, jobConfig);
    }
}
