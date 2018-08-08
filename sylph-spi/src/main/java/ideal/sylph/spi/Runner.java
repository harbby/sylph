package ideal.sylph.spi;

import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.model.PipelinePluginManager;

import java.util.Set;

public interface Runner
{
    Set<JobActuatorHandle> getJobActuators();

    PipelinePluginManager getPluginManager();
}
