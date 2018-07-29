package ideal.sylph.runner.flink;

import ideal.sylph.spi.job.JobHandle;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class FlinkJobHandle
        implements JobHandle
{
    private JobGraph jobGraph;
    private JobParameter jobParameter;

    public static Builder newJob()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final FlinkJobHandle flinkJob = new FlinkJobHandle();

        public Builder setJobParameter(JobParameter parameter)
        {
            flinkJob.jobParameter = parameter;
            return this;
        }

        public Builder setJobGraph(JobGraph jobGraph)
        {
            flinkJob.jobGraph = jobGraph;
            return this;
        }

        public FlinkJobHandle build()
                throws IOException
        {
            requireNonNull(flinkJob.jobGraph, "jobGraph must not null");
            return flinkJob;
        }
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
