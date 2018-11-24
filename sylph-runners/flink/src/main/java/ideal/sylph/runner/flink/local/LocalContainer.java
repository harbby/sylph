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
package ideal.sylph.runner.flink.local;

import ideal.common.jvm.JVMException;
import ideal.common.jvm.JVMLauncher;
import ideal.common.jvm.JVMLaunchers;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class LocalContainer
        implements JobContainer
{
    private static final Logger logger = LoggerFactory.getLogger(LocalContainer.class);

    private final Executor pool = Executors.newSingleThreadExecutor();

    private final JVMLauncher<Boolean> launcher;

    public LocalContainer(JobGraph jobGraph, Collection<URL> deps)
    {
        this.launcher = getExecutor(jobGraph, deps);
    }

    private static JVMLauncher<Boolean> getExecutor(JobGraph jobGraph, Collection<URL> deps)
    {
        return JVMLaunchers.<Boolean>newJvm()
                .setCallable(() -> {
                    MiniExec.execute(jobGraph);
                    return true;
                })
                .setXms("512m")
                .setXmx("512m")
                .setConsole(System.out::println)
                .addUserjars(deps)
                .build();
    }

    @Override
    public String getRunId()
    {
        return "007";
    }

    @Override
    public synchronized Optional<String> run()
            throws Exception
    {
        pool.execute(() -> {
            try {
                launcher.startAndGet();
            }
            catch (JVMException e) {
                throw new RuntimeException(e);
            }
        });
        return Optional.empty();
    }

    @Override
    public void shutdown()
    {
        if (launcher.getProcess() != null) {
            launcher.getProcess().destroy();
        }
    }

    @Override
    public Job.Status getStatus()
    {
        if (launcher.getProcess() == null) {
            return Job.Status.STOP;
        }
        return launcher.getProcess().isAlive() ? Job.Status.RUNNING : Job.Status.STOP;
    }

    @Override
    public void setStatus(Job.Status status)
    {
    }

    @Override
    public String getJobUrl()
    {
        return null;
    }
}
