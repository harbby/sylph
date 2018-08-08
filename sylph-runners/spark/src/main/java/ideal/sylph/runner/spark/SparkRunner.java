package ideal.sylph.runner.spark;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.model.PipelinePluginManager;

import java.util.Set;

public class SparkRunner
        implements Runner
{
    private final Set<JobActuatorHandle> jobActuators;

    @Inject
    public SparkRunner(
            StreamEtlActuator streamEtlActuator,
            Stream2EtlActuator stream2EtlActuator,
            SparkSubmitActuator submitActuator
    )
    {
        this.jobActuators = ImmutableSet.<JobActuatorHandle>builder()
                .add(stream2EtlActuator)
                .add(streamEtlActuator)
                .add(submitActuator)
                .build();
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
