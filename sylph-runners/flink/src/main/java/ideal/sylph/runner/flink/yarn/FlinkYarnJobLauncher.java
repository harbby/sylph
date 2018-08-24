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
import ideal.sylph.runner.flink.FlinkJobHandle;
import ideal.sylph.runner.flink.FlinkRunner;
import ideal.sylph.runner.flink.actuator.JobParameter;
import ideal.sylph.spi.job.Job;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.clusterframework.messages.ShutdownClusterAfterJob;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * 负责和yarn打交道 负责job的管理 提交job 杀掉job 获取job 列表
 */
public class FlinkYarnJobLauncher
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkYarnJobLauncher.class);
    private static final FiniteDuration AKKA_TIMEOUT = new FiniteDuration(1, TimeUnit.MINUTES);

    @Inject
    private YarnClusterConfiguration clusterConf;
    @Inject
    private YarnClient yarnClient;

    public ApplicationId createApplication()
            throws IOException, YarnException
    {
        YarnClientApplication app = yarnClient.createApplication();
        return app.getApplicationSubmissionContext().getApplicationId();
    }

    public YarnClient getYarnClient()
    {
        return yarnClient;
    }

    public void start(Job job, ApplicationId yarnAppId)
            throws Exception
    {
        FlinkJobHandle jobHandle = (FlinkJobHandle) job.getJobHandle();
        final JobParameter jobState = jobHandle.getJobParameter();
        Iterable<Path> userProvidedJars = getUserAdditionalJars(job.getDepends());
        final YarnClusterDescriptor descriptor = new YarnClusterDescriptor(clusterConf, yarnClient, jobState, yarnAppId, userProvidedJars);

        start(descriptor, jobHandle.getJobGraph());
    }

    @VisibleForTesting
    void start(YarnClusterDescriptor descriptor, JobGraph job)
            throws Exception
    {
        YarnClusterClient client = descriptor.deploy();  //这一步提交到yarn
        try {
            client.runDetached(job, null);  //这一步提交到yarn 运行分离
            stopAfterJob(client, job.getJobID());
        }
        finally {
            client.shutdown();
            //清除临时目录
            try {
                FileSystem hdfs = FileSystem.get(clusterConf.conf());
                Path appDir = new Path(clusterConf.appRootDir(), client.getApplicationId().toString());
                hdfs.delete(appDir, true);
            }
            catch (IOException e) {
                logger.error("clear tmp dir is fail", e);
            }
        }
    }

    /**
     * 停止后的工作
     */
    private void stopAfterJob(ClusterClient client, JobID jobID)
    {
        requireNonNull(jobID, "The flinkLoadJob id must not be null");
        try {
            Future<Object> replyFuture =
                    client.getJobManagerGateway().ask(
                            new ShutdownClusterAfterJob(jobID),
                            AKKA_TIMEOUT);
            Await.ready(replyFuture, AKKA_TIMEOUT);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to tell application master to stop"
                    + " once the specified flinkLoadJob has been finished", e);
        }
    }

    /**
     * 获取任务额外需要的jar
     */
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
