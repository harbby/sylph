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
package ideal.sylph.runner.spark;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobEngineHandle;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLClassLoader;

@Name("SparkSubmit")
@Description("spark submit job")
public class SparkSubmitEngine
        implements JobEngineHandle
{
    @NotNull
    @Override
    public Flow formFlow(byte[] flowBytes)
            throws IOException
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @NotNull
    @Override
    public Serializable formJob(String jobId, Flow flow, JobConfig jobConfig, URLClassLoader jobClassLoader)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
