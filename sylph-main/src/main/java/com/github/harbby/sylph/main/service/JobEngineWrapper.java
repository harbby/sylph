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
package com.github.harbby.sylph.main.service;

import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.spi.Runner;
import com.github.harbby.sylph.spi.job.JobEngine;

import static java.util.Objects.requireNonNull;

class JobEngineWrapper
{
    private final JobEngine jobEngine;
    private final String name;
    private final String desc;
    private final Runner runner;

    public JobEngineWrapper(JobEngine jobEngine, Runner runner)
    {
        this.jobEngine = jobEngine;
        this.name = jobEngine.getClass().getAnnotation(Name.class).value();
        this.desc = jobEngine.getClass().getAnnotation(Description.class).value();
        this.runner = requireNonNull(runner, "runner is null");
    }

    public String getName()
    {
        return name;
    }

    public String getDesc()
    {
        return desc;
    }

    public Runner runner()
    {
        return runner;
    }

    public JobEngine getJobEngine()
    {
        return jobEngine;
    }
}
