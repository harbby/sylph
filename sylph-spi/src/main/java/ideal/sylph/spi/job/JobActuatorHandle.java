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

import com.github.harbby.gadtry.jvm.JVMException;
import ideal.sylph.spi.model.PipelinePluginManager;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;

public interface JobActuatorHandle
{
    /**
     * building job
     *
     * @param flow input Flow
     * @param jobClassLoader Independent Job ClassLoader
     * @param jobConfig job config
     * @param jobId job id
     * @return JobHandel
     * @throws JVMException Throw it if the child process fails to compile
     */
    @NotNull
    default JobHandle formJob(String jobId, Flow flow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws Exception
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @NotNull
    Flow formFlow(byte[] flowBytes)
            throws IOException;

    @NotNull
    default Collection<File> parserFlowDepends(Flow flow)
            throws IOException
    {
        return Collections.emptyList();
    }

    @NotNull(message = "getConfigParser() return null")
    default Class<? extends JobConfig> getConfigParser()
    {
        return JobConfig.class;
    }

    default PipelinePluginManager getPluginManager()
    {
        return new PipelinePluginManager() {};
    }
}
