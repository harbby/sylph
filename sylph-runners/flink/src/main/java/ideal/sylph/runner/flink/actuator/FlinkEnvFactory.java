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

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.shaded.org.joda.time.DateTime;

public class FlinkEnvFactory
{
    private static String checkpointDataUri = "hdfs:///tmp/sylph/flink/savepoints/"; //TODO: Need to be organized into a configuration file

    private FlinkEnvFactory() {}

    public static StreamExecutionEnvironment getStreamEnv(JobParameter jobConfig)
    {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();

        if (jobConfig.getCheckpointInterval() > 0) {
            execEnv.enableCheckpointing(jobConfig.getCheckpointInterval());  //default is -1 表示关闭
            execEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); //这是默认值
            execEnv.getCheckpointConfig().setCheckpointTimeout(jobConfig.getCheckpointTimeout()); //10 minutes  this default

            // The maximum number of concurrent checkpoint attempts.
            execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1); //default
        }

        //savePoint
        //default  execEnv.getStateBackend() is null;
        execEnv.setStateBackend((StateBackend) new FsStateBackend(checkpointDataUri + new DateTime().toString("yyyyMMdd")));
        // default  TimeCharacteristic.ProcessingTime
        //execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //set public parallelism
        return execEnv.setParallelism(jobConfig.getParallelism());
    }
}
