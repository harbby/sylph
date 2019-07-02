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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.main.server.ServerMainConfig;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobStore;
import ideal.sylph.spi.model.JobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static ideal.sylph.spi.exception.StandardErrorCode.ILLEGAL_OPERATION;
import static ideal.sylph.spi.job.JobContainer.Status.STARTED_ERROR;
import static ideal.sylph.spi.job.JobContainer.Status.STOP;
import static java.util.Objects.requireNonNull;

public final class JobManager
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JobManager.class);
    private static final int MaxSubmitJobNum = 10;

    private final JobStore jobStore;
    private final JobEngineManager runnerManger;
    private final ServerMainConfig config;

    private final ConcurrentMap<Integer, JobContainer> containers = new ConcurrentHashMap<>();
    private final ExecutorService jobStartPool = Executors.newFixedThreadPool(MaxSubmitJobNum);

    private final Thread monitorService;

    @Autowired
    public JobManager(JobStore jobStore, JobEngineManager runnerManger, ServerMainConfig config)
    {
        this.jobStore = requireNonNull(jobStore, "config is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger is null");
        this.config = config;

        this.monitorService = new Thread(() -> {
            while (true) {
                Thread.currentThread().setName("job_monitor");
                containers.forEach((jobId, container) -> {
                    JobContainer.Status status = container.getStatus();
                    if (status == STOP) {
                        logger.warn("Job {}[{}] state is STOP, Will resubmit", jobId, container.getRunId());
                        this.startJobContainer(jobId, container);
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

    private void startJobContainer(int jobId, JobContainer container)
    {
        Future future = jobStartPool.submit(() -> {
            Optional<String> runId = Optional.empty();
            try {
                Thread.currentThread().setName("job_submit_" + jobId);
                runId = container.run();
            }
            catch (Exception e) {
                Thread thread = Thread.currentThread();
                if (thread.isInterrupted() || Throwables.getRootCause(e) instanceof InterruptedException) {
                    logger.warn("job {} Canceled submission", jobId);
                }
                container.setStatus(STARTED_ERROR);
                logger.warn("job {} start error", jobId, e);
            }

            try {
                if (runId.isPresent()) {
                    jobStore.runJob(jobId, runId.get(), container.getRuntimeType());
                }
            }
            catch (Exception e) {
                logger.error("save job status failed", e);
            }
        });
        container.setFuture(future);
    }

    /**
     * deploy job
     */
    public void startJob(int jobId)
    {
        if (containers.containsKey(jobId)) {
            logger.warn("Job {} already started", jobId);
        }

        synchronized (containers) {
            if (containers.containsKey(jobId)) {
                return;
            }
            JobStore.DbJob job = jobStore.getJob(jobId);
            JobContainer jobContainer = runnerManger.createJobContainer(job, null, config.getRunMode());
            startJobContainer(jobId, jobContainer);
            logger.info("deploy job id:{} name:{}", jobId, job.getJobName());
            JobContainer old = containers.put(jobId, jobContainer);
            if (old != null) {
                logger.warn("");
                old.shutdown();
            }
        }
    }

    /**
     * stop Job
     */
    public void stopJob(int jobId)
            throws Exception
    {
        JobContainer container = containers.remove(jobId);
        if (container != null) {
            logger.warn("stop job {}", jobId);
            jobStore.stopJob(jobId);
            container.shutdown();
        }
    }

    public void saveJob(JobStore.DbJob dbJob)
            throws Exception
    {
        //check
        Job job = runnerManger.compileJob(dbJob);
        job.getJobDAG();
        //save
        dbJob.setConfig(MAPPER.writeValueAsString(job.getConfig()));
        jobStore.saveJob(dbJob);
    }

    public void removeJob(int jobId)
            throws Exception
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
    public JobInfo getJob(int jobId)
    {
        JobStore.DbJob dbJob = jobStore.getJob(jobId);
        return toJobInfo(dbJob);
    }

    public List<JobInfo> listJobs()
    {
        return jobStore.getJobs().stream().map(this::toJobInfo).collect(Collectors.toList());
    }

    private JobInfo toJobInfo(JobStore.DbJob dbJob)
    {
        int jobId = dbJob.getId();
        JobInfo jobInfo = MAPPER.convertValue(dbJob, JobInfo.class);
        Optional<JobContainer> jobContainer = this.getJobContainer(jobId);
        jobContainer.ifPresent(container -> {
            jobInfo.setStatus(container.getStatus());
            jobInfo.setRunId(container.getRunId());
            jobInfo.setAppUrl("/proxy/" + jobId + "/#");
        });
        return jobInfo;
    }

    /**
     * start jobManager
     */
    public void start()
            throws Exception
    {
        monitorService.setDaemon(false);
        monitorService.start();
        //---------  init  read metadata job status  ---------------
        List<JobStore.JobRunState> runningJobs = jobStore.getRunningJobs();
        Map<Integer, JobStore.DbJob> jobs = jobStore.getJobs().stream().collect(Collectors.toMap(JobStore.DbJob::getId, v -> v));
        runningJobs.forEach(jobRunState -> {
            JobStore.DbJob job = requireNonNull(jobs.get(jobRunState.getJobId()), "job " + jobRunState.getJobId() + " not found");
            JobContainer container = runnerManger.createJobContainer(job, jobRunState.getRunId(), jobRunState.getRuntimeType());
            startJobContainer(job.getId(), container);
            containers.put(job.getId(), container);
        });
    }

    /**
     * get running JobContainer
     */
    public Optional<JobContainer> getJobContainer(int jobId)
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
