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
package com.github.harbby.sylph.runner.flink.engines;

import com.github.harbby.sylph.runner.flink.FlinkJobClient;
import com.github.harbby.sylph.runner.flink.FlinkJobConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enabling and Configuring Checkpointing
 * see: https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/state/checkpointing.html#enabling-and-configuring-checkpointing}
 */
public class FlinkEnvFactory
{
    private FlinkEnvFactory() {}

    private static final Logger logger = LoggerFactory.getLogger(FlinkEnvFactory.class);

    /**
     * deprecated
     *
     * @see FlinkJobClient#setJobConfig
     */
    public static StreamExecutionEnvironment createFlinkEnv(FlinkJobConfig jobConfig)
    {
        LocalStreamEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
        if (jobConfig.getCheckpointInterval() > 0) {
            execEnv.enableCheckpointing(jobConfig.getCheckpointInterval());  //default is -1
            //execEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            execEnv.getCheckpointConfig().setCheckpointTimeout(jobConfig.getCheckpointTimeout()); //default 10 minutes

            // The maximum number of concurrent checkpoint attempts.
            //execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1); //default

            // make sure 500 ms of progress happen between checkpoints
            //execEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);  //1000ms

            // enable externalized checkpoints which are retained after job cancellation
            //execEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            //savePoint
            execEnv.getCheckpointConfig().setCheckpointStorage(jobConfig.getCheckpointDir());
        }
        // default  TimeCharacteristic.ProcessingTime
        //execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //set public parallelism
        return execEnv.setParallelism(jobConfig.getParallelism());
    }
}
