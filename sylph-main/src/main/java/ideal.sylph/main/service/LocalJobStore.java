package ideal.sylph.main.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import ideal.sylph.spi.Job;
import ideal.sylph.spi.JobStore;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LocalJobStore
        implements JobStore
{
    private JobStoreConfig config;

    @Inject
    public LocalJobStore(JobStoreConfig config)
    {
        this.config = requireNonNull(config, "JobStore config is null");
    }

    @Override
    public boolean saveJob(@Nonnull Job job)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public Optional<Job> getJob(String jobId)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public List<Job> getJobs()
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public Optional<Job> removeJob(String jobId)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Singleton
    public static class JobStoreConfig
    {
    }
}
