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
package ideal.sylph.runtime.local;

import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LocalContainer
        implements JobContainer
{
    private static final Logger logger = LoggerFactory.getLogger(LocalContainer.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final JVMLaunchers.VmBuilder<Boolean> vmBuilder;

    protected JVMLauncher<Boolean> launcher;
    protected String url = null;

    public LocalContainer(JVMLaunchers.VmBuilder<Boolean> vmBuilder)
    {
        this.vmBuilder = vmBuilder;
        this.launcher = vmBuilder.build();
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
        executor.submit(() -> {
            launcher.startAndGet();
            return true;
        });
        this.setStatus(Job.Status.RUNNING);
        return Optional.empty();
    }

    @Override
    public synchronized void shutdown()
    {
        //url+ "jobs/{job_id}/yarn-cancel/";
        if (launcher.getProcess() != null) {
            launcher.getProcess().destroy();
        }
    }

    @Override
    public void setFuture(Future future)
    {
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
