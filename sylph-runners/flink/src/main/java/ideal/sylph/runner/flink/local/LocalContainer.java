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

import java.lang.reflect.Field;
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
    private String url = null;

    public LocalContainer(JobGraph jobGraph, Collection<URL> deps)
    {
        this.launcher = JVMLaunchers.<Boolean>newJvm()
                .setCallable(() -> {
                    MiniExec.execute(jobGraph);
                    return true;
                })
                .setXms("512m")
                .setXmx("512m")
                .setConsole(line -> {
                    if (url == null && line.contains("Web frontend listening at")) {
                        url = line.split("Web frontend listening at")[1].trim();
                    }
                    System.out.println(line);
                })
                .addUserjars(deps)
                .build();
    }

    @Override
    public String getRunId()
    {
        Process process = launcher.getProcess();
        if (process == null) {
            return "none";
        }
        String system = process.getClass().getName();
        if ("java.lang.UNIXProcess".equals(system)) {
            try {
                Field field = process.getClass().getDeclaredField("pid");
                field.setAccessible(true);
                int pid = (int) field.get(process);
                return String.valueOf(pid);
            }
            catch (NoSuchFieldException | IllegalAccessException ignored) {
            }
        }
        else {
            //todo: widnows get pid
            return "windows";
        }
        return "none";
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
        //url+ "jobs/{job_id}/yarn-cancel/";
        if (launcher.getProcess() != null) {
            launcher.getProcess().destroy();
        }
    }

    @Override
    public Job.Status getStatus()
    {
        Process process = launcher.getProcess();
        if (process == null) {
            return Job.Status.STOP;
        }
        return process.isAlive() ? Job.Status.RUNNING : Job.Status.STOP;
    }

    @Override
    public void setStatus(Job.Status status)
    {
    }

    @Override
    public String getJobUrl()
    {
        return url;
    }
}
