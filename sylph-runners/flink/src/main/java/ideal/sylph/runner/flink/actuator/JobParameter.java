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
package ideal.sylph.runner.flink.actuator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobParameter
{
    private int parallelism = 4;

    @JsonProperty("queue")
    private String queue = "default";

    private int taskManagerMemoryMb = 1024;
    private int taskManagerCount = 2;
    private int taskManagerSlots = 2;
    private int jobManagerMemoryMb = 1024;
    private Set<String> appTags = ImmutableSet.of("sylph", "flink");

    public JobParameter() {}

    public void setQueue(String queue)
    {
        this.queue = queue;
    }

    public void setTaskManagerCount(int taskManagerCount)
    {
        this.taskManagerCount = taskManagerCount;
    }

    public void setTaskManagerMemoryMb(int taskManagerMemoryMb)
    {
        this.taskManagerMemoryMb = taskManagerMemoryMb;
    }

    public void setTaskManagerSlots(int taskManagerSlots)
    {
        this.taskManagerSlots = taskManagerSlots;
    }

    public void setJobManagerMemoryMb(int jobManagerMemoryMb)
    {
        this.jobManagerMemoryMb = jobManagerMemoryMb;
    }

    @JsonProperty("appTags")
    public void setAppTags(String... appTags)
    {
        this.appTags = ImmutableSet.copyOf(appTags);
    }

    @JsonProperty("parallelism")
    public void setParallelism(int parallelism)
    {
        this.parallelism = parallelism;
    }

    @JsonProperty("parallelism")
    public int getParallelism()
    {
        return parallelism;
    }

    /**
     * The name of the queue to which the application should be submitted
     *
     * @return queue
     **/
    @JsonProperty("queue")
    public String getQueue()
    {
        return queue;
    }

    public Set<String> getAppTags()
    {
        return appTags;
    }

    public int getJobManagerMemoryMb()
    {
        return jobManagerMemoryMb;
    }

    public int getTaskManagerSlots()
    {
        return taskManagerSlots;
    }

    public int getTaskManagerCount()
    {
        return taskManagerCount;
    }

    public int getTaskManagerMemoryMb()
    {
        return taskManagerMemoryMb;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobParameter jobParameter = (JobParameter) o;
        return Objects.equals(this.queue, jobParameter.queue) &&
                Objects.equals(this.taskManagerCount, jobParameter.taskManagerCount) &&
                Objects.equals(this.taskManagerMemoryMb, jobParameter.taskManagerMemoryMb);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queue, taskManagerMemoryMb, taskManagerCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queue", queue)
                .add("memory", taskManagerMemoryMb)
                .add("vCores", taskManagerSlots)
                .toString();
    }
}
