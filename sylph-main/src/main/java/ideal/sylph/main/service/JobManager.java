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
package ideal.sylph.main.service;

import com.google.inject.Inject;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ideal.sylph.spi.exception.StandardErrorCode.ILLEGAL_OPERATION;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_START_ERROR;
import static ideal.sylph.spi.job.Job.Status.RUNNING;
import static ideal.sylph.spi.job.Job.Status.STARTING;
import static ideal.sylph.spi.job.Job.Status.START_ERROR;

/**
 * JobManager
 */
public final class JobManager
{
    private static final Logger logger = LoggerFactory.getLogger(JobManager.class);

    @Inject private JobStore jobStore;
    @Inject private RunnerManager runnerManger;
    @Inject private MetadataManager metadataManager;

    //@Inject
    //@Named("max.submitJob")
    private int maxSubmitJob = 10;

    /**
     * 用来做耗时的->任务启动提交到yarn的操作
     */
    private ExecutorService jobStartPool = Executors.newFixedThreadPool(maxSubmitJob);

    /**
     * 上线job
     */
    public synchronized void startJob(String jobId)
    {
        if (runningContainers.containsKey(jobId)) {
            throw new SylphException(JOB_START_ERROR, "Job " + jobId + " already started");
        }
        Job job = this.getJob(jobId).orElseThrow(() -> new SylphException(JOB_START_ERROR, "Job " + jobId + " not found with jobStore"));
        runningContainers.computeIfAbsent(jobId, k -> runnerManger.createJobContainer(job, null));
        logger.info("runningContainers size:{}", runningContainers.size());
    }

    /**
     * 下线Job
     */
    public void stopJob(String jobId)
            throws Exception
    {
        JobContainer container = runningContainers.remove(jobId);
        if (container != null) {
            metadataManager.removeMetadata(jobId);
            container.shutdown();
        }
    }

    public synchronized void saveJob(@NotNull Job job)
    {
        jobStore.saveJob(job);
    }

    public void removeJob(String jobId)
            throws IOException
    {
        if (runningContainers.containsKey(jobId)) {
            throw new SylphException(ILLEGAL_OPERATION, "Can only delete tasks that have been offline");
        }
        jobStore.removeJob(jobId);
    }

    public Optional<Job> getJob(String jobId)
    {
        return jobStore.getJob(jobId);
    }

    @NotNull
    public Collection<Job> listJobs()
    {
        return jobStore.getJobs();
    }

    private final ConcurrentMap<String, JobContainer> runningContainers = new ConcurrentHashMap<>();

    private boolean run;

    private final Thread monitorService = new Thread(() -> {
        while (run) {
            runningContainers.forEach((jobId, container) -> {
                try {
                    Job.Status status = container.getStatus();
                    switch (status) {
                        case RUNNING:
                        case START_ERROR:
                            break;
                        case STARTING:
                            //-------正在启动中-------
                            //TODO: 判断任务启动 用掉耗时 如果大于5分钟 则进行放弃
                            break;
                        default:
                            jobStartPool.submit(() -> {
                                try {
                                    logger.warn("Job {}[{}] Status is {}, Soon to start", jobId,
                                            container.getRunId(), status);
                                    container.setStatus(STARTING);
                                    Optional<String> runResult = container.run();
                                    container.setStatus(RUNNING);
                                    runResult.ifPresent(result -> metadataManager.addMetadata(jobId, result));
                                }
                                catch (Exception e) {
                                    container.setStatus(START_ERROR);
                                    logger.warn("job {} start error", jobId, e);
                                }
                            }); //需要重启 Job
                    }
                }
                catch (Exception e) {
                    logger.warn("Check job {} status error", jobId, e);
                }
            });

            try {
                TimeUnit.SECONDS.sleep(1);
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    });

    /**
     * start jobManager
     */
    public void start()
            throws IOException
    {
        this.run = true;
        monitorService.setDaemon(false);
        monitorService.start();
        //---------  init  read metadata job status  ---------------
        Map<String, String> metadatas = metadataManager.loadMetadata();
        metadatas.forEach((jobId, jobInfo) -> this.getJob(jobId).ifPresent(job -> {
            JobContainer container = runnerManger.createJobContainer(job, jobInfo);
            runningContainers.put(job.getId(), container);
            logger.info("runningContainers size:{}", runningContainers.size());
        }));
    }

    /**
     * get running JobContainer
     */
    public Optional<JobContainer> getJobContainer(@NotNull String jobId)
    {
        return Optional.ofNullable(runningContainers.get(jobId));
    }

    /**
     * get running JobContainer with this runId(demo: yarnAppId)
     */
    public Optional<JobContainer> getJobContainerWithRunId(@NotNull String runId)
    {
        for (JobContainer container : runningContainers.values()) {
            if (runId.equals(container.getRunId())) {
                return Optional.ofNullable(container);
            }
        }
        return Optional.empty();
    }
}
