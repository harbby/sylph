package ideal.sylph.main.service;

import com.google.inject.Inject;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ideal.sylph.spi.exception.StandardErrorCode.ILLEGAL_OPERATION;

/**
 * JobManager
 */
public final class JobManager
{
    private static final Logger logger = LoggerFactory.getLogger(JobManager.class);

    @Inject
    private JobStore jobStore;
    @Inject
    private RunnerManger runnerManger;

    @Min(1)
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
    public void startJob(String jobId)
    {
        this.getJob(jobId).ifPresent(job -> {
            JobContainer container = runnerManger.runJob(job);
            runningContainers.put(jobId, container);
            logger.info("runningContainers size:{}", runningContainers.size());
        });
    }

    /**
     * 下线Job
     */
    public void stopJob(String jobId)
    {
        JobContainer container = runningContainers.remove(jobId);
        if (container != null) {
            container.shutdown();
        }
    }

    public void saveJob(@Nonnull Job job)
    {
        jobStore.saveJob(job);
    }

    public void saveAndStartJob(@Nonnull Job job)
    {
        this.saveJob(job);
        this.startJob(job.getId());
    }

    public Optional<Job> removeJob(String jobId)
    {
        if (runningContainers.containsKey(jobId)) {
            throw new SylphException(ILLEGAL_OPERATION, "Can only delete tasks that have been offline");
        }
        return jobStore.removeJob(jobId);
    }

    public Optional<Job> getJob(String jobId)
    {
        return jobStore.getJob(jobId);
    }

    @Nonnull
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
                    switch (container.getStatus()) {
                        case RUNNING:
                            break;
                        case STARTING:
                            //-------正在启动中-------
                            //TODO: 判断任务启动 用掉耗时 如果大于5分钟 则进行放弃
                            break;
                        case START_ERROR:
                            break;
                        default: {
                            jobStartPool.submit(container::run); //需要重启 Job
                        }
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
    {
        this.run = true;
        monitorService.setDaemon(false);
        monitorService.start();
    }

    /**
     * get running JobContainer
     */
    public Optional<JobContainer> getJobContainer(String jobId)
    {
        return Optional.ofNullable(runningContainers.get(jobId));
    }
}
