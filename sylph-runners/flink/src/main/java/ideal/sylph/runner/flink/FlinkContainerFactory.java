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
package ideal.sylph.runner.flink;

import com.github.harbby.gadtry.aop.AopFactory;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.github.harbby.gadtry.jvm.VmCallable;
import com.github.harbby.gadtry.jvm.VmFuture;
import ideal.sylph.runner.flink.yarn.FlinkConfiguration;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.runtime.local.LocalContainer;
import ideal.sylph.runtime.yarn.YarnJobContainer;
import ideal.sylph.runtime.yarn.YarnModule;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.util.SerializedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ideal.sylph.runner.flink.local.MiniExecutor.FLINK_WEB;
import static ideal.sylph.runner.flink.local.MiniExecutor.createVmCallable;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.CHECKPOINT_DIR_PREFIX;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.METADATA_FILE_NAME;

public class FlinkContainerFactory
        implements ContainerFactory
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkContainerFactory.class);

    private final IocFactory injector = IocFactory.create(new YarnModule(), binder -> {
        binder.bind(FlinkYarnJobLauncher.class).withSingle();
        binder.bind(FlinkConfiguration.class).byCreator(FlinkConfiguration::of).withSingle();
    });

    @Override
    public JobContainer createYarnContainer(Job job, String lastRunid)
    {
        FlinkYarnJobLauncher jobLauncher = injector.getInstance(FlinkYarnJobLauncher.class);
        return YarnJobContainer.builder()
                .setYarnClient(jobLauncher.getYarnClient())
                .setSubmitter(() -> {
                    FlinkJobConfig jobConfig = job.getConfig();
                    JobGraph jobGraph = job.getJobDAG();
                    Path appCheckPath = new Path(jobConfig.getCheckpointDir(), job.getName());
                    if (jobConfig.isEnableSavepoint()) {
                        setSavepoint(jobGraph, appCheckPath, jobLauncher.getYarnClient().getConfig());
                    }
                    return jobLauncher.start(job);
                })
                .setJobClassLoader(job.getJobClassLoader())
                .setLastRunId(lastRunid)
                .build();
    }

    @Override
    public JobContainer createLocalContainer(Job job, String lastRunid)
    {
        AtomicReference<String> url = new AtomicReference<>();
        JVMLauncher<Boolean> launcher = JVMLaunchers.<Boolean>newJvm()
                //.setXms("512m")
                .setXmx("512m")
                .setConsole(line -> {
                    if (url.get() == null && line.contains(FLINK_WEB)) {
                        url.set(line.split(FLINK_WEB)[1].trim());
                    }
                    System.out.println(line);
                })
                .notDepThisJvmClassPath()
                .addUserjars(job.getDepends())
                .build();
        YarnConfiguration yarnConfiguration = injector.getInstance(YarnConfiguration.class);
        return new LocalContainer()
        {
            @Override
            public String getJobUrl()
            {
                return url.get();
            }

            @Override
            public VmFuture startAsyncExecutor()
                    throws Exception
            {
                FlinkJobConfig jobConfig = job.getConfig();
                Path appCheckPath = new Path(jobConfig.getCheckpointDir(), job.getName());
                JobGraph jobGraph = job.getJobDAG();
                url.set(null);
                if (jobConfig.isEnableSavepoint()) {
                    setSavepoint(jobGraph, appCheckPath, yarnConfiguration);
                }
                VmCallable<Boolean> taskCallable = createVmCallable(jobGraph);
                return launcher.startAsync(taskCallable);
            }
        };
    }

    public static void setSavepoint(JobGraph jobGraph, Path appCheckPath, Configuration hadoopConf)
            throws Exception
    {
        //How to use `savepoints` to restore a job
        jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.none());
        FileSystem fileSystem = FileSystem.get(hadoopConf);
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

    public static void setJobConfig(JobGraph jobGraph, FlinkJobConfig jobConfig, ClassLoader jobClassLoader, String jobId)
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
        CheckpointCoordinatorConfiguration config = new CheckpointCoordinatorConfiguration(
                jobConfig.getCheckpointInterval(), //default is -1 表示关闭 建议1minutes
                jobConfig.getCheckpointTimeout(),  //10 minutes  this default
                jobConfig.getMinPauseBetweenCheckpoints(), // make sure 1000 ms of progress happen between checkpoints
                1,      // The maximum number of concurrent checkpoint attempts.
                RETAIN_ON_CANCELLATION,
                true,   //CheckpointingMode.EXACTLY_ONCE //这是默认值
                true,  //todo: cfg.isPreferCheckpointForRecovery()
                0  //todo: cfg.getTolerableCheckpointFailureNumber()
        );

        //set checkPoint
        //default  execEnv.getStateBackend() is null default is asynchronousSnapshots = true;
        //see: https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/state/checkpointing.html#enabling-and-configuring-checkpointing
        Path appCheckPath = new Path(jobConfig.getCheckpointDir(), jobId);
        StateBackend stateBackend = new FsStateBackend(appCheckPath.toString(), true)
        {
            @Override
            public FsStateBackend configure(org.apache.flink.configuration.Configuration config, ClassLoader classLoader)
            {
                FsStateBackend fsStateBackend = super.configure(config, classLoader);
                return AopFactory.proxy(FsStateBackend.class).byInstance(fsStateBackend)
                        .returnType(CheckpointStorage.class)
                        .around(proxyContext -> {
                            //Object value = proxyContext.proceed();
                            JobID jobId = (JobID) proxyContext.getArgs()[0];
                            logger.info("mock {}", proxyContext.getMethod());
                            return new SylphFsCheckpointStorage(getCheckpointPath(), getSavepointPath(), jobId, getMinFileSizeThreshold());
                        });
            }
        };
        JobCheckpointingSettings settings = jobGraph.getCheckpointingSettings();
        JobCheckpointingSettings checkSettings = new JobCheckpointingSettings(
                settings.getVerticesToTrigger(),
                settings.getVerticesToAcknowledge(),
                settings.getVerticesToConfirm(),
                config,
                new SerializedValue<>(stateBackend),
                settings.getMasterHooks()
        );
        jobGraph.setSnapshotSettings(checkSettings);
    }
}
