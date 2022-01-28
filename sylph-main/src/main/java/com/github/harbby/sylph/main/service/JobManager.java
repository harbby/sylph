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
package com.github.harbby.sylph.main.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.sylph.main.SylphException;
import com.github.harbby.sylph.main.dao.JobRepository;
import com.github.harbby.sylph.main.dao.StatusRepository;
import com.github.harbby.sylph.main.server.ServerMainConfig;
import com.github.harbby.sylph.spi.JobClient;
import com.github.harbby.sylph.spi.dao.Job;
import com.github.harbby.sylph.spi.dao.JobInfo;
import com.github.harbby.sylph.spi.dao.JobRunState;
import com.github.harbby.sylph.spi.job.DeployResponse;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class JobManager
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JobManager.class);

    private final JobRepository<RuntimeException> jobStore;
    private final StatusRepository<RuntimeException> statusRepository;

    private final ServerMainConfig config;
    private final JobCompiler jobCompiler;
    private final JobEngineManager jobEngineManager;
    private final BlockingQueue<Runnable> submitPool;
    private final ExecutorService service;
    private final Thread checkThread;

    @Autowired
    public JobManager(JobRepository<RuntimeException> jobStore,
            StatusRepository<RuntimeException> statusRepository,
            ServerMainConfig config,
            JobCompiler jobCompiler,
            JobEngineManager jobEngineManager)
    {
        this.jobStore = requireNonNull(jobStore, "config is null");
        this.statusRepository = requireNonNull(statusRepository);
        this.config = config;
        this.jobCompiler = jobCompiler;
        this.jobEngineManager = jobEngineManager;
        this.submitPool = new LinkedBlockingQueue<>(1024);
        this.service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, submitPool);
        this.checkThread = new Thread(() -> {
            while (true) {
                try {
                    checkAndRestartJobs();
                }
                catch (Exception e) {
                    logger.warn("check job status failed", e);
                }
                try {
                    TimeUnit.MINUTES.sleep(1);
                }
                catch (InterruptedException e) {
                    break;
                }
            }
        });
        checkThread.setName("check_jobs_thread");
        checkThread.start();
    }

    private class DeployJobTask
            implements Runnable
    {
        private final Job job;

        private DeployJobTask(Job job)
        {
            this.job = job;
        }

        @Override
        public void run()
        {
            try {
                var jobEngine = jobEngineManager.getJobEngine(job.getType());
                var jobConfig = jobEngine.runner().analyzeConfig(job.getConfig());
                var jobDag = jobCompiler.compileJob(job);
                jobEngine.runner().getFrameworkJars().forEach(jobDag::addJar);
                JobClient jobClient = jobEngine.runner().getJobSubmitter();
                DeployResponse response = jobClient.deployJobOnYarn(jobDag, jobConfig);
                statusRepository.updateRunIdById(job.getId(), response.getRunId(), response.getAppWeb());
                urlCache.put(String.valueOf(job.getId()), response.getAppWeb());
                urlCache.put(response.getRunId(), response.getAppWeb());
                logger.info("job {} submit succeed", job.getId());
            }
            catch (Exception e) {
                logger.error("submit job {} failed", job.getId(), e);
                statusRepository.updateStateById(job.getId(), JobRunState.Status.DEPLOY_FAILED);
            }
        }
    }

    private void checkAndRestartJobs()
    {
        var groupJobs = statusRepository.findByState(JobRunState.Status.RUNNING)
                .stream().collect(Collectors.groupingBy(JobRunState::getType));
        groupJobs.forEach((engineName, jobs) -> {
            if (!jobs.isEmpty()) {
                var runner = jobEngineManager.getJobEngine(engineName).runner();
                var runIds = jobs.stream().collect(Collectors.toMap(JobRunState::getJobId, JobRunState::getRunId));
                Map<Integer, JobRunState.Status> clusterJobs = Collections.emptyMap();
                try {
                    clusterJobs = runner.getJobSubmitter().getAllJobStatus(runIds);
                }
                catch (Exception e) {
                    logger.warn("check runner {} jobs statues failed", runner.getClass().getName(), e);
                }
                clusterJobs.entrySet().stream().filter(x -> x.getValue() != JobRunState.Status.RUNNING).forEach(entry -> {
                    var jobId = entry.getKey();
                    var jobOption = statusRepository.findById(jobId);
                    if (jobOption.isPresent()) {
                        var job = jobOption.get();
                        if (job.getStatus() == JobRunState.Status.RUNNING) {
                            if ((System.currentTimeMillis() - job.getModifyTime()) > TimeUnit.MINUTES.toMillis(10)) {
                                try {
                                    statusRepository.updateStateById(jobId, JobRunState.Status.STOP);
                                    deployJob(jobId);
                                }
                                catch (Exception e) {
                                    logger.warn("auto restart job {} failed ", jobId, e);
                                }
                            }
                            else {
                                statusRepository.updateStateById(jobId, JobRunState.Status.RUNNING_FAILED);
                            }
                        }
                    }
                });
                //child running failed
                statusRepository.findByState(JobRunState.Status.RUNNING_FAILED).forEach(job -> {
                    if ((System.currentTimeMillis() - job.getModifyTime()) > TimeUnit.MINUTES.toMillis(10)) {
                        try {
                            statusRepository.updateStateById(job.getJobId(), JobRunState.Status.STOP);
                            deployJob(job.getJobId());
                        }
                        catch (Exception e) {
                            logger.warn("auto restart job {} failed ", job.getJobId(), e);
                        }
                    }
                });
            }
        });
    }

    /**
     * deploy job
     */
    public synchronized void deployJob(int jobId)
            throws Exception
    {
        var option = statusRepository.findById(jobId);
        if (option.isPresent()) {
            JobRunState.Status status = option.get().getStatus();
            if (status == JobRunState.Status.DEPLOYING || status == JobRunState.Status.RUNNING) {
                return;
            }
        }
        var job = jobStore.getById(jobId);
        JobRunState jobRunState = new JobRunState();
        jobRunState.setJobId(jobId);
        jobRunState.setRunId(null);
        jobRunState.setRuntimeType("YARN");
        jobRunState.setStatus(JobRunState.Status.DEPLOYING);
        jobRunState.setType(job.getType());
        statusRepository.save(jobRunState);
        service.submit(new DeployJobTask(job));
    }

    /**
     * stop Job
     */
    public void stopJob(int jobId)
            throws Exception
    {
        var jobRunState = statusRepository.findById(jobId);
        if (jobRunState.isPresent()) {
            var job = jobRunState.get();
            logger.warn("stop job {}", jobId);
            JobEngineWrapper jobEngine = jobEngineManager.getJobEngine(job.getType());
            if (job.getStatus() == JobRunState.Status.RUNNING) {
                jobEngine.runner().getJobSubmitter().closeJobOnYarn(job.getRunId());
                statusRepository.deleteById(jobId);
            }
            else {
                statusRepository.updateStateById(jobId, JobRunState.Status.STOP);
                logger.warn("stop job failed, the job status is {}", job.getStatus());
            }
        }
    }

    public void saveJob(Job job)
            throws Exception
    {
        //check
        jobCompiler.compileJob(job);
        jobStore.save(job);
    }

    public void removeJob(int jobId)
    {
        if (statusRepository.findById(jobId).isPresent()) {
            throw new SylphException("Unable to delete running job");
        }
        jobStore.deleteById(jobId);
    }

    /**
     * Get the compiled job
     *
     * @param jobId job id
     * @return Job Optional
     */
    public JobInfo getJob(int jobId)
    {
        var job = jobStore.getById(jobId);
        return toJobInfo(job);
    }

    private final Cache<String, String> urlCache = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .maximumSize(100)
            .build();

    public String getJobWebUi(String jobIdOrRunId)
            throws Exception
    {
        return urlCache.get(jobIdOrRunId, () -> {
            var job = jobIdOrRunId.startsWith("application_") ?
                    statusRepository.findByRunId(jobIdOrRunId) :
                    statusRepository.findById(Integer.parseInt(jobIdOrRunId));
            return job.map(JobRunState::getWebUi)
                    .orElseThrow(() -> new IllegalStateException("job " + jobIdOrRunId + "not is deployed"));
        });
    }

    public List<JobInfo> listJobs()
    {
        return jobStore.findAll().stream().map(this::toJobInfo).collect(Collectors.toList());
    }

    private JobInfo toJobInfo(Job job)
    {
        var jobId = job.getId();
        var jobInfo = MAPPER.convertValue(job, JobInfo.class);
        var jobRunState = statusRepository.findById(jobId);
        if (jobRunState.isPresent()) {
            jobInfo.setStatus(jobRunState.get().getStatus());
            jobInfo.setRunId(jobRunState.get().getRunId());
            jobInfo.setAppUrl("/proxy/" + jobId + "/#");
        }
        //jobInfo.setAppUrl(jobRunState.get().getWebUi());
        return jobInfo;
    }

    public void start()
    {
    }
}
