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
package ideal.sylph.runner.flink.local;

import com.github.harbby.gadtry.jvm.VmCallable;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * see {@link org.apache.flink.streaming.api.environment.LocalStreamEnvironment#execute(String)}
 */
public class MiniExec
{
    private MiniExec() {}

    private static final Logger logger = LoggerFactory.getLogger(MiniExec.class);

    public static JobExecutionResult execute(JobGraph jobGraph)
            throws Exception
    {
        jobGraph.setAllowQueuedScheduling(true);

        Configuration configuration = new Configuration();
        configuration.addAll(jobGraph.getJobConfiguration());
        configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "0");

        // add (and override) the settings with what the user defined
        configuration.addAll(jobGraph.getJobConfiguration());

        if (!configuration.contains(RestOptions.PORT)) {
            configuration.setInteger(RestOptions.PORT, 0);
        }

        int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

        MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                .build();

        if (logger.isInfoEnabled()) {
            logger.info("Running job on local embedded Flink mini cluster");
        }

        MiniCluster miniCluster = new MiniCluster(cfg);

        try {
            miniCluster.start();
            configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().getPort());

            return miniCluster.executeJobBlocking(jobGraph);
        }
        finally {
            miniCluster.close();
        }
    }

    public static VmCallable<Boolean> getLocalRunner(JobGraph jobGraph)
    {
        return () -> {
            MiniExec.execute(jobGraph);
            return true;
        };
    }
}
