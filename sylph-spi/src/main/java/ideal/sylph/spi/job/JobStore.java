package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.util.Collection;
import java.util.Optional;

public interface JobStore
{
    public boolean saveJob(@NotNull Job job);

    public Optional<Job> getJob(String jobId);

    public Collection<Job> getJobs();

    public Optional<Job> removeJob(String jobId);

    public void loadJobs();
}
