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

import com.github.harbby.gadtry.ioc.Autowired;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static ideal.sylph.spi.exception.StandardErrorCode.ILLEGAL_OPERATION;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_START_ERROR;
import static ideal.sylph.spi.job.Job.Status.RUNNING;
import static ideal.sylph.spi.job.Job.Status.STARTED_ERROR;
import static ideal.sylph.spi.job.Job.Status.STARTING;
import static ideal.sylph.spi.job.Job.Status.STOP;

public final class JobManager
{
    private static final Logger logger = LoggerFactory.getLogger(JobManager.class);
    private static final int MaxSubmitJobNum = 10;

    private final JobStore jobStore;
    private final RunnerManager runnerManger;
    private final MetadataManager metadataManager;

    private final ConcurrentMap<String, JobContainer> containers = new ConcurrentHashMap<>();
    private final ExecutorService jobStartPool = Executors.newFixedThreadPool(MaxSubmitJobNum);

    private final Thread monitorService;

    @Autowired
    public JobManager(JobStore jobStore, RunnerManager runnerManger, MetadataManager metadataManager)
    {
        this.jobStore = jobStore;
        this.runnerManger = runnerManger;
        this.metadataManager = metadataManager;

        this.monitorService = new Thread(() -> {
            while (true) {
                Thread.currentThread().setName("job_monitor");
                containers.forEach((jobId, container) -> {
                    try {
                        Job.Status status = container.getStatus();
                        if (status == STOP) {
                            logger.warn("Job {}[{}] state is {}, Will resubmit", jobId, container.getRunId(), status);
                            container.setStatus(STARTING);
                            Future future = jobStartPool.submit(() -> {
                                try {
                                    Thread.currentThread().setName("job_submit_" + jobId);
                                    Optional<String> runId = container.run();
                                    container.setStatus(RUNNING);
                                    runId.ifPresent(result -> metadataManager.addMetadata(jobId, result));
                                }
                                catch (Exception e) {
                                    container.setStatus(STARTED_ERROR);
                                    logger.warn("job {} start error", jobId, e);
                                }
                            });
                            container.setFuture(future);
                        }
                    }
                    catch (Exception e) {
                        logger.error("jobId {}, Check state failed", jobId, e);
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
    }

    /**
     * deploy job
     */
    public void startJob(String jobId)
    {
        if (containers.containsKey(jobId)) {
            throw new SylphException(JOB_START_ERROR, "Job " + jobId + " already started");
        }
        Job job = this.getJob(jobId).orElseThrow(() -> new SylphException(JOB_START_ERROR, "Job " + jobId + " not found with jobStore"));
        containers.computeIfAbsent(jobId, k -> runnerManger.createJobContainer(job, null));
        logger.info("deploy job :{}", jobId);
    }

    /**
     * stop Job
     */
    public void stopJob(String jobId)
            throws Exception
    {
        JobContainer container = containers.remove(jobId);
        if (container != null) {
            logger.warn("job {} Cancel submission", jobId);
            metadataManager.removeMetadata(jobId);
            container.shutdown();
        }
    }

    public void saveJob(Job job)
    {
        jobStore.saveJob(job);
    }

    public void removeJob(String jobId)
            throws IOException
    {
        if (containers.containsKey(jobId)) {
            throw new SylphException(ILLEGAL_OPERATION, "Unable to delete running job");
        }
        jobStore.removeJob(jobId);
    }

    /**
     * Get the compiled job
     *
     * @param jobId job id
     * @return Job Optional
     */
    public Optional<Job> getJob(String jobId)
    {
        return jobStore.getJob(jobId);
    }

    public Collection<Job> listJobs()
    {
        return jobStore.getJobs();
    }

    /**
     * start jobManager
     */
    public void start()
            throws IOException
    {
        monitorService.setDaemon(false);
        monitorService.start();
        //---------  init  read metadata job status  ---------------
        Map<String, String> metadatas = metadataManager.loadMetadata();
        metadatas.forEach((jobId, jobInfo) -> this.getJob(jobId).ifPresent(job -> {
            JobContainer container = runnerManger.createJobContainer(job, jobInfo);
            containers.put(job.getId(), container);
        }));
    }

    /**
     * get running JobContainer
     */
    public Optional<JobContainer> getJobContainer(@NotNull String jobId)
    {
        return Optional.ofNullable(containers.get(jobId));
    }

    /**
     * get running JobContainer with this runId(demo: yarnAppId)
     */
    public Optional<JobContainer> getJobContainerWithRunId(@NotNull String runId)
    {
        for (JobContainer container : containers.values()) {
            if (runId.equals(container.getRunId())) {
                return Optional.of(container);
            }
        }
        return Optional.empty();
    }
}
