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

import com.github.harbby.gadtry.jvm.VmFuture;
import com.sun.jna.Platform;
import ideal.sylph.spi.job.JobContainerAbs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.Future;

public abstract class LocalContainer
        extends JobContainerAbs
{
    private static final Logger logger = LoggerFactory.getLogger(LocalContainer.class);

    private VmFuture vmFuture;

    @Override
    public String getRunId()
    {
        if (vmFuture == null) {
            return "none";
        }
        Process process = vmFuture.getVmProcess();
        String system = process.getClass().getName();
        if ("java.lang.UNIXProcess".equals(system)) {
            long pid = vmFuture.getPid();
            return String.valueOf(pid);
        }
        else {
            //todo: widnows get pid not return "windows";
            logger.debug("#### win 获取 " + getProcessId(process));
            return getProcessId(process);
        }
    }

    @Override
    protected String deploy()
            throws Exception
    {
        this.vmFuture = startAsyncExecutor();
        //return String.valueOf(vmFuture.getPid());
        return this.getRunId();
    }

    public abstract VmFuture startAsyncExecutor()
            throws Exception;

    @Override
    public synchronized void shutdown()
    {
        //url+ "jobs/{job_id}/yarn-cancel/";
        if (vmFuture != null) {
            vmFuture.cancel();
        }
    }

    @Override
    public String getRuntimeType()
    {
        return "local";
    }

    @Override
    public void setFuture(Future future)
    {
    }

    @Override
    public Status getStatus()
    {
        if (super.getStatus() == Status.RUNNING) {
            if (vmFuture.isRunning()) {
                return Status.RUNNING;
            }
            else {
                try {
                    vmFuture.get();
                }
                catch (Exception e) {
                    logger.error("", e);
                }
                return Status.STOP;
            }
        }
        return super.getStatus();
    }

    public static String getProcessId(Process process)
    {
        long pid = -1;
        Field field = null;
        if (Platform.isWindows()) {
            try {
                field = process.getClass().getDeclaredField("handle");
                field.setAccessible(true);
                pid = Kernel32.INSTANCE.getProcessId((Long) field.get(process));
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        else if (Platform.isLinux() || Platform.isAIX()) {
            try {
                Class<?> clazz = Class.forName("java.lang.UNIXProcess");
                field = clazz.getDeclaredField("pid");
                field.setAccessible(true);
                pid = (Integer) field.get(process);
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
        }
        return String.valueOf(pid);
    }
}
