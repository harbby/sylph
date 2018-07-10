package ideal.sylph.spi;

import java.util.Set;

public interface Runner
{
    Set<JobActuator> create(RunnerContext context);
}
