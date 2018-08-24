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
package ideal.sylph.runner.flink;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.sylph.runner.flink.actuator.FlinkStreamEtlActuator;
import ideal.sylph.runner.flink.actuator.FlinkStreamSqlActuator;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.model.PipelinePluginManager;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class FlinkRunner
        implements Runner
{
    public static final String FLINK_DIST = "flink-dist";

    private final Set<JobActuatorHandle> jobActuators;
    private final PipelinePluginManager pluginManager;

    @Inject
    public FlinkRunner(
            FlinkStreamEtlActuator streamEtlActuator,
            FlinkStreamSqlActuator streamSqlActuator,
            PipelinePluginManager pluginManager
    )
    {
        this.jobActuators = ImmutableSet.<JobActuatorHandle>builder()
                .add(requireNonNull(streamEtlActuator, "streamEtlActuator is null"))
                .add(requireNonNull(streamSqlActuator, "streamEtlActuator is null"))
                .build();
        this.pluginManager = pluginManager;
    }

    @Override
    public Set<JobActuatorHandle> getJobActuators()
    {
        return jobActuators;
    }

    @Override
    public PipelinePluginManager getPluginManager()
    {
        return pluginManager;
    }
}
