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

import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;

public interface JobActuatorHandle
{
    @NotNull
    default JobHandle formJob(String jobId, Flow flow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws IOException
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @NotNull
    default Flow formFlow(byte[] flowBytes)
            throws IOException
    {
        return EtlFlow.load(flowBytes);
    }

    @NotNull
    default Collection<File> parserFlowDepends(Flow flow)
            throws IOException
    {
        return Collections.emptyList();
    }

    @NotNull(message = "getConfigParser() return null")
    default Class<? extends JobConfig> getConfigParser()
            throws IOException
    {
        return JobConfig.class;
    }

    default JobContainer createJobContainer(@NotNull Job job, String jobInfo)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
