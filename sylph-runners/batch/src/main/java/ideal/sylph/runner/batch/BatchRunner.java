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
package ideal.sylph.runner.batch;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.model.PipelinePluginManager;

import java.util.Set;

public class BatchRunner
        implements Runner
{
    private final Set<JobActuatorHandle> jobActuators;

    @Inject
    BatchRunner(
            BatchEtlActuator batchEtlActuator
    )
    {
        this.jobActuators = ImmutableSet.of(batchEtlActuator);
    }

    @Override
    public Set<JobActuatorHandle> getJobActuators()
    {
        return jobActuators;
    }

    @Override
    public PipelinePluginManager getPluginManager()
    {
        return new PipelinePluginManager() {};
    }
}
