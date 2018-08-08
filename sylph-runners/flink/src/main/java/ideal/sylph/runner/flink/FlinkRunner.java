package ideal.sylph.runner.flink;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.sylph.runner.flink.runtime.FlinkStreamEtlActuator;
import ideal.sylph.runner.flink.runtime.FlinkStreamSqlActuator;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.model.PipelinePluginManager;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class FlinkRunner
        implements Runner
{
    public static final String FLINK_DIST = "flink-dist";

    private final Set<JobActuatorHandle> jobActuators;
    private final PipelinePluginManager pluginManager;

    @Inject
    public FlinkRunner(
            FlinkStreamEtlActuator streamEtlActuator,
            FlinkStreamSqlActuator streamSqlActuator,
            PipelinePluginManager pluginManager
    )
    {
        this.jobActuators = ImmutableSet.<JobActuatorHandle>builder()
                .add(requireNonNull(streamEtlActuator, "streamEtlActuator is null"))
                .add(requireNonNull(streamSqlActuator, "streamEtlActuator is null"))
                .build();
        this.pluginManager = pluginManager;
    }

    @Override
    public Set<JobActuatorHandle> getJobActuators()
    {
        return jobActuators;
    }

    @Override
    public PipelinePluginManager getPluginManager()
    {
        return pluginManager;
    }
}
