package ideal.sylph.runner.batch;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.model.PipelinePluginManager;

import java.util.Set;

public class BatchRunner
        implements Runner
{
    private final Set<JobActuatorHandle> jobActuators;

    @Inject
    BatchRunner(
            BatchEtlActuator batchEtlActuator
    )
    {
        this.jobActuators = ImmutableSet.of(batchEtlActuator);
    }

    @Override
    public Set<JobActuatorHandle> getJobActuators()
    {
        return jobActuators;
    }

    @Override
    public PipelinePluginManager getPluginManager()
    {
        return new PipelinePluginManager() {};
    }
}
