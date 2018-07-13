package ideal.sylph.runner.flink.runtime;

import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;

@Name("graph_job")
@Description("this is graph job Actuator")
public class GraphActuator
        implements JobActuator
{
    @Override
    public void execJob(Job job)
    {
    }
}
