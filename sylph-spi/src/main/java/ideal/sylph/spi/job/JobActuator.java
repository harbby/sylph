package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

public interface JobActuator
{
    @NotNull
    default Job formJob(String jobId, Flow flow)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    default JobContainer execJob(@NotNull Job job)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
