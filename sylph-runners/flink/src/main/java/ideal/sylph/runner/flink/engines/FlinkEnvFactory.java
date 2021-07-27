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
package ideal.sylph.runner.flink.engines;

import com.github.harbby.gadtry.aop.AopGo;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.runner.flink.SylphFsCheckpointStorage;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.harbby.gadtry.aop.mock.MockGoArgument.any;

/**
 * Enabling and Configuring Checkpointing
 * see: https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/state/checkpointing.html#enabling-and-configuring-checkpointing}
 */
public class FlinkEnvFactory
{
    private FlinkEnvFactory() {}

    private static final Logger logger = LoggerFactory.getLogger(FlinkEnvFactory.class);

    /**
     * deprecated see: {@link ideal.sylph.runner.flink.FlinkContainerFactory#setJobConfig)}
     */
    @Deprecated
    public static StreamExecutionEnvironment setJobConfig(StreamExecutionEnvironment execEnv, FlinkJobConfig jobConfig, String jobId)
    {
        if (jobConfig.getCheckpointInterval() > 0) {
            execEnv.enableCheckpointing(jobConfig.getCheckpointInterval());  //default is -1 表示关闭 建议1minutes
            execEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); //这是默认值
            execEnv.getCheckpointConfig().setCheckpointTimeout(jobConfig.getCheckpointTimeout()); //10 minutes  this default

            // The maximum number of concurrent checkpoint attempts.
            execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1); //default

            // make sure 500 ms of progress happen between checkpoints
            execEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(jobConfig.getMinPauseBetweenCheckpoints());  //1000ms

            // enable externalized checkpoints which are retained after job cancellation
            execEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            //savePoint
            //default  execEnv.getStateBackend() is null;
            //see: https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/state/checkpointing.html#enabling-and-configuring-checkpointing
            //execEnv.setStateBackend((StateBackend) new FsStateBackend(appCheckPath.toString()));
            Path appCheckPath = new Path(jobConfig.getCheckpointDir(), jobId);
            //execEnv.setStateBackend((StateBackend) new FsStateBackend(appCheckPath.toString(), true));
            StateBackend stateBackend = new FsStateBackend(appCheckPath.toString(), true)
            {
                @Override
                public FsStateBackend configure(ReadableConfig config, ClassLoader classLoader)
                {
                    FsStateBackend fsStateBackend = super.configure(config, classLoader);
                    return AopGo.proxy(FsStateBackend.class).byInstance(fsStateBackend)
                            .aop(binder -> {
                                binder.doAround(proxyContext -> {
                                    //Object value = proxyContext.proceed();
                                    JobID jobId = (JobID) proxyContext.getArgs()[0];
                                    logger.info("mock {}", proxyContext.getMethod());
                                    return new SylphFsCheckpointStorage(getCheckpointPath(), getSavepointPath(), jobId, getMinFileSizeThreshold());
                                }).when().createCheckpointStorage(any());
                            })
                            .build();
                }
            };
            execEnv.setStateBackend(stateBackend);
        }
        // default  TimeCharacteristic.ProcessingTime
        //execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //set public parallelism
        return execEnv.setParallelism(jobConfig.getParallelism());
    }
}
