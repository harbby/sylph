package ideal.sylph.runner.flink.runtime;

import ideal.sylph.spi.Job;
import ideal.sylph.spi.JobActuator;
import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;

import javax.annotation.Nonnull;

import java.io.File;

@Name("graph_job")
@Description("this is graph job Actuator")
public class GraphActuator
        implements JobActuator
{
    @Nonnull
    @Override
    public Job formJob(File jobDir)
    {
        return null;
    }

    @Override
    public void execJob(Job job)
    {
    }
}
