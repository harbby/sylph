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
package ideal.sylph.spi.job;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.Collection;
import java.util.function.Supplier;

public final class Job
{
    private final Integer id;
    private final String name;
    private final File workDir;
    private final Collection<URL> depends;
    private final ClassLoader jobClassLoader;

    private final Supplier<Serializable> jobDAG;
    private final JobConfig jobConfig;

    public Job(Integer id,
            String name,
            File workDir,
            Collection<URL> depends,
            ClassLoader jobClassLoader,
            Supplier<Serializable> jobDAG,
            JobConfig jobConfig)
    {
        this.id = id;
        this.name = name;
        this.workDir = workDir;
        this.depends = depends;
        this.jobClassLoader = jobClassLoader;
        this.jobDAG = jobDAG;
        this.jobConfig = jobConfig;
    }

    public int getId()
    {
        return id;
    }

    public String getName()
    {
        return this.name;
    }

    public String getFullName()
    {
        return String.format("job%s:%s", getId(), getName());
    }

    public File getWorkDir()
    {
        return this.workDir;
    }

    public Collection<URL> getDepends()
    {
        return depends;
    }

    public ClassLoader getJobClassLoader()
    {
        return jobClassLoader;
    }

    public <T extends Serializable> T getJobDAG()
    {
        return (T) jobDAG.get();
    }

    public <T extends JobConfig> T getConfig()
    {
        return (T) jobConfig;
    }
}
