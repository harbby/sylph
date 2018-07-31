package ideal.sylph.spi;

import ideal.sylph.spi.job.JobActuatorHandle;

import java.util.Set;

public interface Runner
{
    Set<JobActuatorHandle> create(RunnerContext context);
}
