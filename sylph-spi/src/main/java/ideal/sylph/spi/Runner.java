package ideal.sylph.spi;

import ideal.sylph.spi.job.JobActuator;

import java.util.Set;

public interface Runner
{
    Set<JobActuator> create(RunnerContext context);
}
