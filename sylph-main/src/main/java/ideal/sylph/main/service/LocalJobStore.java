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
import com.google.inject.Singleton;
import ideal.sylph.common.base.Throwables;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobStore;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static ideal.sylph.main.util.PropertiesUtil.loadProperties;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static ideal.sylph.spi.exception.StandardErrorCode.SAVE_JOB_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class LocalJobStore
        implements JobStore
{
    private static final Logger logger = LoggerFactory.getLogger(LocalJobStore.class);
    private final JobStoreConfig config;
    private final RunnerManager runnerManger;

    private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();

    @Inject
    public LocalJobStore(
            JobStoreConfig config,
            RunnerManager runnerManger
    )
    {
        this.config = requireNonNull(config, "JobStore config is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger config is null");
    }

    @Override
    public void saveJob(@Nonnull Job job)
    {
        File jobDir = job.getWorkDir();
        job.getId();
        try {
            Flow flow = job.getFlow();
            File yaml = new File(jobDir, "job.flow");
            File typeFile = new File(jobDir, "job.type");

            //TODO: save?
            String jobType = job.getActuatorName();
            FileUtils.writeStringToFile(yaml, flow.toString(), UTF_8);
            FileUtils.writeStringToFile(typeFile, "type=" + jobType, UTF_8);

            jobs.put(job.getId(), job);
            logger.info("save job {} ok", job.getId());
        }
        catch (IOException e) {
            throw new SylphException(SAVE_JOB_ERROR, "save " + job.getId() + " failed", e);
        }
    }

    @Override
    public Optional<Job> getJob(String jobId)
    {
        return Optional.ofNullable(jobs.get(jobId));
    }

    @Override
    public Collection<Job> getJobs()
    {
        return jobs.values();
    }

    @Override
    public Job removeJob(String jobId)
            throws IOException
    {
        Job job = requireNonNull(jobs.remove(jobId), jobId + " is not exists");
        FileUtils.deleteDirectory(job.getWorkDir());  //先删除然后在复制
        return job;
    }

    /**
     * load local jobs dir job
     */
    @Override
    public void loadJobs()
    {
        File jobsDir = new File("jobs");
        List<File> errorJob = new ArrayList<>();
        Stream.of(requireNonNull(jobsDir.listFiles(), "jobs Dir is not exists"))
                .parallel()
                .forEach(jobDir -> {
                    try {
                        final File typeFile = new File(jobDir, "job.type");
                        checkArgument(typeFile.exists() && typeFile.isFile(), typeFile + " is not exists or isDirectory");
                        Map<String, String> jobProps = loadProperties(typeFile);
                        String jobType = requireNonNull(jobProps.get("type"), "jobProps arg type is null");
                        try {
                            byte[] flowBytes = Files.readAllBytes(Paths.get(new File(jobDir, "job.flow").toURI()));
                            Job job = runnerManger.formJobWithFlow(jobDir.getName(), flowBytes, jobType);
                            jobs.put(job.getId(), job);
                        }
                        catch (IOException e) {
                            throw new SylphException(JOB_BUILD_ERROR, "loadding job " + jobDir + " job.flow fail", e);
                        }
                    }
                    catch (Exception e) {
                        logger.warn("job {} 加载失败", jobDir, Throwables.getRootCause(e));
                        errorJob.add(jobDir);
                    }
                });
        logger.info("loading ok jobs {},but fail load {}", jobs.size(), errorJob);
    }

    /**
     * 绑定 JobStoreConfig
     */
    @Singleton
    public static class JobStoreConfig
    {
    }
}
