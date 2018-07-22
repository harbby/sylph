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
import static ideal.sylph.spi.exception.StandardErrorCode.SAVE_JOB_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class LocalJobStore
        implements JobStore
{
    private static final Logger logger = LoggerFactory.getLogger(LocalJobStore.class);
    private final JobStoreConfig config;
    private final RunnerManger runnerManger;

    private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();

    @Inject
    public LocalJobStore(
            JobStoreConfig config,
            RunnerManger runnerManger
    )
    {
        this.config = requireNonNull(config, "JobStore config is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger config is null");
    }

    @Override
    public void saveJob(@Nonnull Job job)
    {
        File jobDir = new File("jobs/" + job.getId());
        job.getId();
        try {
            Flow flow = job.getFlow();
            File yaml = new File(jobDir, "job.yaml");
            File typeFile = new File(jobDir, "type.job");

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
    public Optional<Job> removeJob(String jobId)
    {
        throw new UnsupportedOperationException("this method have't support!");
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
                        final File typeFile = new File(jobDir, "type.job");
                        checkArgument(typeFile.exists() && typeFile.isFile(), typeFile + " is not exists or isDirectory");
                        Map<String, String> jobProps = loadProperties(typeFile);
                        Job job = runnerManger.formJobWithDir(jobDir, jobProps);
                        jobs.put(job.getId(), job);
                    }
                    catch (Exception e) {
                        logger.warn("job {} 加载失败", jobDir, Throwables.getRootCause(e));
                        errorJob.add(jobDir);
                    }
                });
        logger.info("loading ok jobs {},but fail load {}", jobs.size(), errorJob);
    }

    /**
     * 隐式绑定 JobStoreConfig
     */
    @Singleton
    public static class JobStoreConfig
    {
    }
}
