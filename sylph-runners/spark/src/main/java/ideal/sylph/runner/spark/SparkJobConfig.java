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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import ideal.sylph.spi.job.JobConfig;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkJobConfig
{
    private String driverMemory = "1600m";
    private int driverCores = 1;
    private int numExecutors = 2;
    private String executorMemory = "1600m";
    private int executorCores = 1;

    private String queue = "default";

    private Map<String, String> sparkConf = new HashMap<>();

    @JsonProperty("driver-cores")
    public void setDriverCores(int driverCores)
    {
        this.driverCores = driverCores;
    }

    @JsonProperty("driver-memory")
    public void setDriverMemory(String driverMemory)
    {
        this.driverMemory = driverMemory;
    }

    @JsonProperty("executor-cores")
    public void setExecutorCores(int executorCores)
    {
        this.executorCores = executorCores;
    }

    @JsonProperty("executor-memory")
    public void setExecutorMemory(String executorMemory)
    {
        this.executorMemory = executorMemory;
    }

    @JsonProperty("num-executors")
    public void setNumExecutors(int numExecutors)
    {
        this.numExecutors = numExecutors;
    }

    @JsonProperty("queue")
    public void setQueue(String queue)
    {
        this.queue = queue;
    }

//    @JsonProperty("sparkConf")
//    public void setSparkConf(Map<String, String> sparkConf)
//    {
//        this.sparkConf = sparkConf;
//    }

    @JsonProperty("driver-cores")
    public int getDriverCores()
    {
        return driverCores;
    }

    @JsonProperty("driver-memory")
    public String getDriverMemory()
    {
        return driverMemory;
    }

    @JsonProperty("executor-cores")
    public int getExecutorCores()
    {
        return executorCores;
    }

    @JsonProperty("executor-memory")
    public String getExecutorMemory()
    {
        return executorMemory;
    }

    @JsonProperty("num-executors")
    public int getNumExecutors()
    {
        return numExecutors;
    }

    @JsonProperty("queue")
    public String getQueue()
    {
        return queue;
    }

//    @JsonProperty("sparkConf")
//    public Map<String, String> getSparkConf()
//    {
//        return sparkConf;
//    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SparkConfReader
            extends JobConfig
    {
        private final SparkJobConfig config;

        @JsonCreator
        public SparkConfReader(
                @JsonProperty("type") String type,
                @JsonProperty("config") SparkJobConfig jobConfig
        )
        {
            super(type);
            this.config = requireNonNull(jobConfig, "jobConfig is null");
        }

        @Override
        public SparkJobConfig getConfig()
        {
            return config;
        }
    }
}
