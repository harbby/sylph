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

import ideal.sylph.spi.job.JobHandle;
import org.apache.flink.runtime.jobgraph.JobGraph;

import static java.util.Objects.requireNonNull;

public class FlinkJobHandle
        implements JobHandle
{
    private JobGraph jobGraph;

    public FlinkJobHandle(JobGraph jobGraph)
    {
        this.jobGraph = requireNonNull(jobGraph, "jobGraph is null");
    }

    public JobGraph getJobGraph()
    {
        return jobGraph;
    }
}
