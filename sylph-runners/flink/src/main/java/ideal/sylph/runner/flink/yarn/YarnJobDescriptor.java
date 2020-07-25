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
package ideal.sylph.runner.flink.yarn;

import ideal.sylph.runner.flink.FlinkJobConfig;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class YarnJobDescriptor
        extends YarnClusterDescriptor {
    private static final String APPLICATION_TYPE = "Sylph_FLINK";
    private static final int MAX_ATTEMPT = 2;

    private final YarnClient yarnClient;
    private final FlinkJobConfig appConf;
    private final String jobName;

    YarnJobDescriptor(
            Configuration flinkConf,
            YarnClient yarnClient,
            YarnConfiguration yarnConfiguration,
            FlinkJobConfig appConf,
            String jobId) {
        super(flinkConf, yarnConfiguration, yarnClient, YarnClientYarnClusterInformationRetriever.create(yarnClient), false);
        this.jobName = jobId;
        this.yarnClient = yarnClient;
        this.appConf = appConf;
    }

    @Override
    protected String getYarnSessionClusterEntrypoint() {
        return YarnSessionClusterEntrypoint.class.getName();
    }

    /**
     * 提交到yarn时 任务启动入口类
     * YarnApplicationMasterRunner
     */
    @Override
    protected String getYarnJobClusterEntrypoint() {
        return YarnJobClusterEntrypoint.class.getName();
    }

    public ClusterClient<ApplicationId> deploy(JobGraph jobGraph, boolean detached)
            throws Exception {
        // this is required because the slots are allocated lazily
        //jobGraph.setAllowQueuedScheduling(true);

        Configuration flinkConfiguration = getFlinkConfiguration();

        flinkConfiguration.setString(YarnConfigOptions.APPLICATION_NAME, jobName);
        flinkConfiguration.setString(YarnConfigOptions.APPLICATION_QUEUE, appConf.getQueue());
        flinkConfiguration.setString(YarnConfigOptions.APPLICATION_TYPE, APPLICATION_TYPE);
        flinkConfiguration.setString(YarnConfigOptions.APPLICATION_TAGS, String.join(",", appConf.getAppTags()));

        //flinkConfiguration.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, " ");
        //flinkConfiguration.set(CoreOptions.FLINK_JVM_OPTIONS, " ");
        //flinkConfiguration.set(CoreOptions.FLINK_TM_JVM_OPTIONS, " ");

        flinkConfiguration.setInteger(YarnConfigOptions.APPLICATION_ATTEMPTS.key(), MAX_ATTEMPT);
        flinkConfiguration.setInteger(YarnConfigOptions.APP_MASTER_VCORES, 1);  //default 1

        //flinkConfiguration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, ...);

//        flinkConfiguration.setString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, "logback.xml");

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(appConf.getJobManagerMemoryMb())
                .setSlotsPerTaskManager(appConf.getTaskManagerSlots())
                .setTaskManagerMemoryMB(appConf.getTaskManagerMemoryMb())
                .createClusterSpecification();
        return this.deployJobCluster(clusterSpecification, jobGraph, detached).getClusterClient();
    }
}
