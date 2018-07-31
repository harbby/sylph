package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

public interface JobStore
{
    public void saveJob(@NotNull Job job);

    public Optional<Job> getJob(String jobId);

    public Collection<Job> getJobs();

    public Job removeJob(String jobId)
            throws IOException;

    public void loadJobs();
}
