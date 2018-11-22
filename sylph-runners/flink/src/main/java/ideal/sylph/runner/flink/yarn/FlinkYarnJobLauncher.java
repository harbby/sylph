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

import com.google.inject.Inject;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.runner.flink.FlinkJobHandle;
import ideal.sylph.runner.flink.FlinkRunner;
import ideal.sylph.runner.flink.actuator.JobParameter;
import ideal.sylph.spi.job.Job;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 负责和yarn打交道 负责job的管理 提交job 杀掉job 获取job 列表
 */
public class FlinkYarnJobLauncher
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkYarnJobLauncher.class);

    @Inject
    private YarnClusterConfiguration clusterConf;
    @Inject
    private YarnClient yarnClient;

    public YarnClient getYarnClient()
    {
        return yarnClient;
    }

    public ClusterClient<ApplicationId> start(Job job)
            throws Exception
    {
        FlinkJobHandle jobHandle = (FlinkJobHandle) job.getJobHandle();
        JobParameter jobConfig = ((FlinkJobConfig) job.getConfig()).getConfig();

        Iterable<Path> userProvidedJars = getUserAdditionalJars(job.getDepends());
        final YarnClusterDescriptor descriptor = new YarnClusterDescriptor(
                clusterConf,
                yarnClient,
                jobConfig,
                job.getId(),
                userProvidedJars);
        JobGraph jobGraph = jobHandle.getJobGraph();
        //todo: How to use `savepoints` to restore a job
        //jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath("hdfs:///tmp/sylph/apps/savepoints"));
        return start(descriptor, jobGraph);
    }

    private ClusterClient<ApplicationId> start(YarnClusterDescriptor descriptor, JobGraph job)
            throws Exception
    {
        ApplicationId applicationId = null;
        try {
            ClusterClient<ApplicationId> client = descriptor.deploy();  //create app master
            applicationId = client.getClusterId();
            ClusterSpecification specification = new ClusterSpecification.ClusterSpecificationBuilder()
                    .setMasterMemoryMB(1024)
                    .setNumberTaskManagers(2)
                    .setSlotsPerTaskManager(2)
                    .setTaskManagerMemoryMB(1024)
                    .createClusterSpecification();
            client.runDetached(job, null);  //submit graph to yarn appMaster 并运行分离
            return client;
        }
        catch (Exception e) {
            if (applicationId != null) {
                yarnClient.killApplication(applicationId);
            }
            throw e;
        }
        finally {
            //Clear temporary directory
            try {
                if (applicationId != null) {
                    FileSystem hdfs = FileSystem.get(clusterConf.yarnConf());
                    Path appDir = new Path(clusterConf.appRootDir(), applicationId.toString());
                    hdfs.delete(appDir, true);
                }
            }
            catch (IOException e) {
                logger.error("clear tmp dir is fail", e);
            }
        }
    }

    private static Iterable<Path> getUserAdditionalJars(Collection<URL> userJars)
    {
        return userJars.stream().map(jar -> {
            try {
                final URI uri = jar.toURI();
                final File file = new File(uri);
                if (file.exists() && file.isFile()) {
                    return new Path(uri);
                }
            }
            catch (Exception e) {
                logger.warn("add user jar error with URISyntaxException {}", jar);
            }
            return null;
        }).filter(x -> Objects.nonNull(x) && !x.getName().startsWith(FlinkRunner.FLINK_DIST)).collect(Collectors.toList());
    }
}
