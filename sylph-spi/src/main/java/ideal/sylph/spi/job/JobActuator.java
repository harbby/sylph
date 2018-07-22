package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.util.Optional;

public interface JobActuator
{
    @NotNull
    default Job formJob(String jobId, Flow flow)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    default JobContainer createJobContainer(@NotNull Job job, Optional<String> jobInfo)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
