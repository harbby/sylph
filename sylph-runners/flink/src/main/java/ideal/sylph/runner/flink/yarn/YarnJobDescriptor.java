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

import com.github.harbby.gadtry.base.Throwables;
import ideal.sylph.runner.flink.FlinkJobConfig;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.lang.reflect.Method;

public class YarnJobDescriptor
        extends YarnClusterDescriptor
{
    public static final String APPLICATION_TYPE = "SYLPH_FLINK";
    public static final int MAX_ATTEMPT = 2;
    private static final Method deployInternal = Throwables.noCatch(() -> {
        Method m = YarnClusterDescriptor.class.getDeclaredMethod("deployInternal", ClusterSpecification.class, String.class, String.class, JobGraph.class, boolean.class);
        m.setAccessible(true);
        return m;
    });
    private final FlinkJobConfig appConf;
    private final String jobName;

    YarnJobDescriptor(
            Configuration flinkConf,
            YarnClient yarnClient,
            YarnConfiguration yarnConfiguration,
            FlinkJobConfig appConf,
            String jobId)
    {
        super(flinkConf, yarnConfiguration, yarnClient, YarnClientYarnClusterInformationRetriever.create(yarnClient), false);
        this.jobName = jobId;
        this.appConf = appConf;
    }

    @Override
    protected String getYarnSessionClusterEntrypoint()
    {
        return YarnSessionClusterEntrypoint.class.getName();
    }

    /**
     * 提交到yarn时 任务启动入口类
     * YarnApplicationMasterRunner
     */
    @Override
    protected String getYarnJobClusterEntrypoint()
    {
        return YarnJobClusterEntrypoint.class.getName();
    }

    public ClusterClient<ApplicationId> deploy(JobGraph jobGraph, boolean detached)
            throws Exception
    {
        // this is required because the slots are allocated lazily
        //jobGraph.setAllowQueuedScheduling(true);
        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(appConf.getJobManagerMemoryMb())
                .setSlotsPerTaskManager(appConf.getTaskManagerSlots())
                .setTaskManagerMemoryMB(appConf.getTaskManagerMemoryMb())
                .createClusterSpecification();

        //checkState(System.getenv(ConfigConstants.ENV_FLINK_PLUGINS_DIR) != null, "flink1.12 must set env FLINK_PLUGINS_DIR"); //flink1.12 need

        @SuppressWarnings("unchecked")
        ClusterClientProvider<ApplicationId> clientProvider = (ClusterClientProvider<ApplicationId>) deployInternal.invoke(this, clusterSpecification,
                jobName
                , getYarnJobClusterEntrypoint(),
                jobGraph, detached);
        //return this.deployJobCluster(clusterSpecification, jobGraph, detached).getClusterClient();
        return clientProvider.getClusterClient();
    }
}
