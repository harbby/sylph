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
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.io.Serializable;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobParameter
        implements Serializable
{
    private int parallelism = 4;
    private String queue = "default";

    private int taskManagerMemoryMb = 1024;
    private int taskManagerCount = 2;
    private int taskManagerSlots = 2;
    private int jobManagerMemoryMb = 1024;
    private Set<String> appTags = ImmutableSet.of("Sylph", "Flink");

    /**
     * checkpoint
     */
    private int checkpointInterval = -1;   //see: CheckpointConfig.checkpointInterval;
    private long checkpointTimeout = CheckpointConfig.DEFAULT_TIMEOUT;
    private boolean enableSavepoint = false;
    private String checkpointDir = "hdfs:///tmp/sylph/flink/savepoints/";
    private long minPauseBetweenCheckpoints = CheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS;

    public JobParameter() {}

    @JsonProperty("queue")
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

    public int getTaskManagerCount()
    {
        return taskManagerCount;
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

    public void setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints)
    {
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
    }

    public long getMinPauseBetweenCheckpoints()
    {
        return minPauseBetweenCheckpoints;
    }

    @JsonProperty("enableSavepoint")
    public boolean isEnableSavepoint()
    {
        return enableSavepoint;
    }

    @JsonProperty("enableSavepoint")
    public void setEnableSavepoint(boolean enableSavepoint)
    {
        this.enableSavepoint = enableSavepoint;
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
                .add("taskManagerCount", taskManagerCount)
                .add("jobManagerMemoryMb", jobManagerMemoryMb)
                .add("parallelism", parallelism)
                .add("vCores", taskManagerSlots)
                .add("checkpointInterval", checkpointInterval)
                .add("checkpointDir", checkpointDir)
                .add("checkpointTimeout", checkpointTimeout)
                .toString();
    }
}
