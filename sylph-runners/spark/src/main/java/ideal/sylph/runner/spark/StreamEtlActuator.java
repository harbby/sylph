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

import com.google.inject.Inject;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.EtlFlow;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.PipelinePluginManager;

import javax.validation.constraints.NotNull;

@Name("Spark_StreamETL")
@Description("spark1.x spark streaming StreamETL")
public class StreamEtlActuator
        extends Stream2EtlActuator
{
    @Inject private PipelinePluginManager pluginManager;

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow flow, DirClassLoader jobClassLoader)
    {
        return JobHelper.build1xJob(jobId, (EtlFlow) flow, jobClassLoader, pluginManager);
    }
}
