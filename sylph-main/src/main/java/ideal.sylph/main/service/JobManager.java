package ideal.sylph.main.service;

import com.google.inject.Inject;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobStore;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ideal.sylph.spi.exception.StandardErrorCode.CONNECTION_ERROR;
import static ideal.sylph.spi.exception.StandardErrorCode.ILLEGAL_OPERATION;
import static ideal.sylph.spi.job.Job.Status.RUNNING;
import static ideal.sylph.spi.job.Job.Status.STARTING;
import static ideal.sylph.spi.job.Job.Status.START_ERROR;
import static ideal.sylph.spi.job.Job.Status.STOP;

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
    @Inject
    private YarnClient yarnClient;

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
            var container = new YarnJobContainer(jobId, () -> runnerManger.runJob(job));
            runContainers.put(jobId, container);
        });
    }

    /**
     * 下线Job
     */
    public void stopJob(String jobId)
    {
        throw new UnsupportedOperationException("this method have't support!");
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
        if (runContainers.containsKey(jobId)) {
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

    private final ConcurrentMap<String, JobContainer> runContainers = new ConcurrentHashMap<>();

    private boolean run;

    /**
     * Container that runs the job
     */
    private class YarnJobContainer
            implements JobContainer
    {
        private final String jobId;
        private ApplicationId applicationId;
        private Job.Status status = STOP;
        private Runnable runnable;

        private YarnJobContainer(String jobId, Runnable runnable)
        {
            this.jobId = jobId;
//            this.applicationId = applicationId;
        }

        @Override
        public String getId()
        {
            return applicationId.toString();
        }

        @Override
        public Job.Status getStatus()
        {
            YarnApplicationState state = getYarnAppStatus(applicationId);
            if (YarnApplicationState.ACCEPTED.equals(state) || YarnApplicationState.RUNNING.equals(state)) {  //运行良好
                this.status = RUNNING;
            }
            return status;
        }

        @Override
        public void run()
        {
            logger.warn("Job {}[{}]状态为:{} 即将进行重新启动", jobId, this.getId(), this.getStatus());
            this.status = STARTING;
            try {
                runnable.run();
            }
            catch (Exception e) {
                this.status = START_ERROR;
                logger.warn("任务{}启动失败:", jobId, e);
            }
        }

        /**
         * 获取yarn Job运行情况
         */
        private YarnApplicationState getYarnAppStatus(ApplicationId applicationId)
        {
            try {
                ApplicationReport app = yarnClient.getApplicationReport(applicationId); //获取某个指定的任务
                return app.getYarnApplicationState();
            }
            catch (ApplicationNotFoundException e) {  //app 不存在与yarn上面
                return null;
            }
            catch (YarnException | IOException e) {
                throw new SylphException(CONNECTION_ERROR, e);
            }
        }
    }

    private Thread monitorService = new Thread(() -> {
        while (run) {
            runContainers.forEach((jobId, container) -> {
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
                        case STOP:
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
                TimeUnit.SECONDS.sleep(2);
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
}
