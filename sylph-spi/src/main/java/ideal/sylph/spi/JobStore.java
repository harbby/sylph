package ideal.sylph.spi;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Optional;

public interface JobStore
{
    public boolean saveJob(@Nonnull Job job);

    public Optional<Job> getJob(String jobId);

    public List<Job> getJobs();

    public Optional<Job> removeJob(String jobId);
}
