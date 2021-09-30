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
package com.github.harbby.sylph.runner.flink;

import com.github.harbby.sylph.spi.JobClient;
import com.github.harbby.sylph.spi.dao.JobRunState;
import com.github.harbby.sylph.spi.job.DeployResponse;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobDag;
import com.github.harbby.sylph.yarn.YarnClientFactory;
import com.github.harbby.sylph.yarn.YarnDeployResponse;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.sylph.runner.flink.FlinkRunner.APPLICATION_TYPE;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.CHECKPOINT_DIR_PREFIX;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME;

public class FlinkJobClient
        implements JobClient
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkJobClient.class);
    private final File flinkDistJar;
    private final File flinkConfDirectory;
    private final List<URL> flinkLibJars;

    public FlinkJobClient(File flinkDistJar, List<URL> flinkLibJars, File flinkConfDirectory)
    {
        this.flinkDistJar = flinkDistJar;
        this.flinkConfDirectory = flinkConfDirectory;
        this.flinkLibJars = flinkLibJars;
    }

    @Override
    public DeployResponse deployJobOnYarn(JobDag<?> job, JobConfig jobConfig)
            throws Exception
    {
        try (YarnClient yarnClient = YarnClientFactory.createYarnClient()) {
            FlinkJobConfig flinkJobConfig = (FlinkJobConfig) jobConfig;
            JobGraph jobGraph = (JobGraph) job.getGraph();
            jobGraph.addJars(job.getJars());
            jobGraph.addJars(flinkLibJars);

            if (flinkJobConfig.getLastGraphId().isPresent()) {
                trySetSavepoint(jobGraph, flinkJobConfig.getCheckpointDir(),
                        flinkJobConfig.getLastGraphId().get(),
                        yarnClient.getConfig());
            }
            ApplicationId runId = this.deploy(yarnClient, jobGraph, flinkJobConfig);
            String webUi = yarnClient.getApplicationReport(runId).getTrackingUrl();
            return new YarnDeployResponse(runId, webUi);
        }
    }

    @Override
    public void closeJobOnYarn(String runId)
            throws Exception
    {
        try (YarnClient yarnClient = YarnClientFactory.createYarnClient()) {
            yarnClient.killApplication(Apps.toAppID(runId));
        }
    }

    @Override
    public Map<Integer, JobRunState.Status> getAllJobStatus(Map<Integer, String> runIds)
            throws Exception
    {
        try (YarnClient yarnClient = YarnClientFactory.createYarnClient()) {
            GetApplicationsRequest request = GetApplicationsRequest.newInstance();
            request.setApplicationTypes(Collections.singleton(APPLICATION_TYPE));
            request.setApplicationStates(EnumSet.of(YarnApplicationState.RUNNING));
            Set<String> yarnApps = yarnClient.getApplications(request).stream()
                    .map(x -> x.getApplicationId().toString()).collect(Collectors.toSet());

            return runIds.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    v -> yarnApps.contains(v.getValue()) ? JobRunState.Status.RUNNING : JobRunState.Status.STOP));
        }
    }

    private ApplicationId deploy(YarnClient yarnClient, JobGraph jobGraph, FlinkJobConfig jobConfig)
            throws Exception
    {
        org.apache.flink.configuration.Configuration flinkConfiguration = createFlinkConfiguration(jobConfig);
        flinkConfiguration.setString(YarnConfigOptions.FLINK_DIST_JAR, flinkDistJar.getPath());

        final YarnClusterDescriptor descriptor = new YarnClusterDescriptor(
                flinkConfiguration,
                new YarnConfiguration(yarnClient.getConfig()),
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);
        //descriptor.addShipFiles(job.getDepends());
        YarnLogConfigUtil.setLogConfigFileInConfig(flinkConfiguration, flinkConfDirectory.getPath());
        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();
        logger.info("start flink job {}", jobGraph.getJobID());
        try (ClusterClient<ApplicationId> client = descriptor.deployJobCluster(clusterSpecification, jobGraph, true).getClusterClient()) {
            return client.getClusterId();
        }
        catch (Throwable e) {
            logger.error("submitting job {} failed", jobGraph.getJobID(), e);
            throw e;
        }
    }

    private org.apache.flink.configuration.Configuration createFlinkConfiguration(FlinkJobConfig jobConfig)
    {
        final org.apache.flink.configuration.Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration();
        flinkConfiguration.addAll(org.apache.flink.configuration.Configuration.fromMap(jobConfig.getOtherMap()));

        //flinkConfiguration.setString(YarnConfigOptions.APPLICATION_NAME, job.getName());
        flinkConfiguration.setString(YarnConfigOptions.APPLICATION_QUEUE, jobConfig.getQueue());
        flinkConfiguration.setString(YarnConfigOptions.APPLICATION_TYPE, APPLICATION_TYPE);
        flinkConfiguration.setString(YarnConfigOptions.APPLICATION_TAGS, String.join(",", jobConfig.getAppTags()));

        flinkConfiguration.setInteger(YarnConfigOptions.APPLICATION_ATTEMPTS.key(), 2);
        flinkConfiguration.setInteger(YarnConfigOptions.APP_MASTER_VCORES, 1);  //default 1

        //set tm vcores
        flinkConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobConfig.getTaskManagerSlots());
        //flinkConfiguration.setInteger(YarnConfigOptions.VCORES, appConf.getTaskManagerSlots());

        //flinkConfiguration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, ...);

        flinkConfiguration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, new MemorySize((long) jobConfig.getJobManagerMemoryMb() * 1024 * 1024));
        flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, new MemorySize((long) jobConfig.getTaskManagerMemoryMb() * 1024 * 1024));

        return flinkConfiguration;
    }

    public static void trySetSavepoint(JobGraph jobGraph, String checkpointDir, String lastGraphId, Configuration hadoopConf)
            throws Exception
    {
        //How to use `savepoints` to restore a job
        Path appCheckPath = new Path(checkpointDir, lastGraphId);
        jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.none());
        try (FileSystem fileSystem = FileSystem.get(hadoopConf)) {
            if (!fileSystem.exists(appCheckPath)) {
                return;
            }
            List<FileStatus> appCheckDirFiles = Stream.of(fileSystem.listStatus(appCheckPath))
                    .filter(file -> file.getPath().getName().startsWith(CHECKPOINT_DIR_PREFIX))
                    .sorted((x, y) -> Long.compare(y.getModificationTime(), x.getModificationTime()))
                    .collect(Collectors.toList());
            for (FileStatus fileStatus : appCheckDirFiles) {
                Path metadataFile = new Path(fileStatus.getPath().toString(), METADATA_FILE_NAME);
                if (fileSystem.exists(metadataFile)) {
                    //allowNonRestoredState （可选）：布尔值，指定如果保存点包含无法映射回作业的状态，是否应拒绝作业提交。 default is false
                    logger.info("Find Savepoint {}", metadataFile);
                    jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(metadataFile.toString(), true));
                    break;
                }
            }
        }
    }

    public static void setJobConfig(JobGraph jobGraph, FlinkJobConfig jobConfig, ClassLoader jobClassLoader)
            throws IOException, ClassNotFoundException
    {
        // set Parallelism
        ExecutionConfig executionConfig = jobGraph.getSerializedExecutionConfig().deserializeValue(jobClassLoader);
        executionConfig.setParallelism(jobConfig.getParallelism());
        jobGraph.setExecutionConfig(executionConfig);

        // set check config
        if (jobConfig.getCheckpointInterval() <= 0) {
            return;
        }
        //---setting flink job
        CheckpointCoordinatorConfiguration config = CheckpointCoordinatorConfiguration.builder()
                .setCheckpointInterval(jobConfig.getCheckpointInterval())  //default is -1
                .setCheckpointTimeout(jobConfig.getCheckpointTimeout())    //10 minutes  this default
                //.setMinPauseBetweenCheckpoints(jobConfig.getMinPauseBetweenCheckpoints())  // make sure 1000 ms of progress happen between checkpoints
                .setMaxConcurrentCheckpoints(1)   // The maximum number of concurrent checkpoint attempts.
                .setCheckpointRetentionPolicy(RETAIN_ON_CANCELLATION)
                .setExactlyOnce(true)   //default value
                .setUnalignedCheckpointsEnabled(true)
                .setTolerableCheckpointFailureNumber(0)
                .build();

        CheckpointStorage checkpointStorage = new FileSystemCheckpointStorage(jobConfig.getCheckpointDir());
        JobCheckpointingSettings settings = jobGraph.getCheckpointingSettings();
        JobCheckpointingSettings checkSettings = new JobCheckpointingSettings(
                config,
                settings.getDefaultStateBackend(),
                settings.isChangelogStateBackendEnabled(),
                new SerializedValue<>(checkpointStorage),
                settings.getMasterHooks());
        jobGraph.setSnapshotSettings(checkSettings);
    }
}
