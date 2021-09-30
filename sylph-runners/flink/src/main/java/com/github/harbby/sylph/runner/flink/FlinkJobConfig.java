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
package com.github.harbby.sylph.runner.flink;

import com.github.harbby.sylph.spi.job.JobConfig;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.shaded.guava30.com.google.common.base.MoreObjects.toStringHelper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkJobConfig
        implements Serializable, JobConfig
{
    private int parallelism = 4;
    private String queue = "default";

    private int taskManagerMemoryMb = 1024;
    private int taskManagerSlots = 2;
    private int jobManagerMemoryMb = 1024;
    private Set<String> appTags = ImmutableSet.of("Sylph", "Flink");

    /**
     * checkpoint
     */
    private int checkpointInterval = -1;   //see: CheckpointConfig.checkpointInterval;
    private long checkpointTimeout = CheckpointConfig.DEFAULT_TIMEOUT;
    private String checkpointDir = "hdfs:///tmp/sylph/flink/savepoints/";

    public FlinkJobConfig() {}

    @JsonProperty("queue")
    public void setQueue(String queue)
    {
        this.queue = queue;
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
     * App submitted to the queue used by yarn
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

    public int getTaskManagerMemoryMb()
    {
        return taskManagerMemoryMb;
    }

    public int getCheckpointInterval()
    {
        return checkpointInterval;
    }

    public void setCheckpointDir(String checkpointDir)
    {
        this.checkpointDir = checkpointDir;
    }

    public String getCheckpointDir()
    {
        return checkpointDir;
    }

    public void setCheckpointInterval(int checkpointInterval)
    {
        this.checkpointInterval = checkpointInterval;
    }

    public long getCheckpointTimeout()
    {
        return checkpointTimeout;
    }

    public void setCheckpointTimeout(long checkpointTimeout)
    {
        this.checkpointTimeout = checkpointTimeout;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queue", queue)
                .add("memory", taskManagerMemoryMb)
                .add("jobManagerMemoryMb", jobManagerMemoryMb)
                .add("parallelism", parallelism)
                .add("vCores", taskManagerSlots)
                .add("checkpointInterval", checkpointInterval)
                .add("checkpointDir", checkpointDir)
                .add("checkpointTimeout", checkpointTimeout)
                .toString();
    }

    @Override
    public Map<String, String> getOtherMap()
    {
        return Collections.emptyMap();
    }

    public Optional<String> getLastGraphId()
    {
        return Optional.empty();
    }
}
