package ideal.sylph.main.service;

import com.google.inject.Inject;
import ideal.sylph.spi.Job;
import ideal.sylph.spi.JobStore;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * JobManager
 */
public class JobManager
{
    private final JobStore jobStore;
    private final RunnerManger runnerManger;

    @Inject
    public JobManager(
            JobStore jobStore,
            RunnerManger runnerManger
    )
    {
        this.jobStore = requireNonNull(jobStore, "jobStore is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger is null");
    }

    /**
     * 上线job
     */
    public void startJob(String jobId)
    {
        this.getJob(jobId).ifPresent(runnerManger::runJob);
        throw new UnsupportedOperationException("this method have't support!");
    }

    /**
     * 下线Job
     */
    public void stopJob(String jobId)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    public synchronized void saveJob(@Nonnull Job job)
    {
        jobStore.saveJob(job);
    }

    public synchronized void saveAndStartJob(@Nonnull Job job)
    {
        this.saveJob(job);
        this.startJob(job.getJobId());
    }

    public Optional<Job> removeJob(String jobId)
    {
        return jobStore.removeJob(jobId);
    }

    public Optional<Job> getJob(String jobId)
    {
        return jobStore.getJob(jobId);
    }

    @Nonnull
    public List<Job> listJobs()
    {
        return jobStore.getJobs();
    }
}
