package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.net.URLClassLoader;
import java.util.Optional;

public interface JobActuatorHandle
{
    @NotNull
    default JobHandle formJob(String jobId, Flow flow, URLClassLoader jobClassLoader)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    default JobContainer createJobContainer(@NotNull Job job, Optional<String> jobInfo)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
