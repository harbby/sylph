package ideal.sylph.runner.flink;

import ideal.sylph.runner.flink.actuator.JobParameter;
import ideal.sylph.spi.job.JobHandle;
import org.apache.flink.runtime.jobgraph.JobGraph;

import static java.util.Objects.requireNonNull;

public class FlinkJobHandle
        implements JobHandle
{
    private JobGraph jobGraph;
    private JobParameter jobParameter;

    public FlinkJobHandle(JobGraph jobGraph, JobParameter jobParameter)
    {
        this.jobGraph = requireNonNull(jobGraph, "jobGraph is null");
        this.jobParameter = requireNonNull(jobParameter, "jobParameter is null");
    }

    public JobGraph getJobGraph()
    {
        return jobGraph;
    }

    public JobParameter getJobParameter()
    {
        return jobParameter;
    }
}
