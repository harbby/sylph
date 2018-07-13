package ideal.sylph.runner.flink;

import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.validation.constraints.NotNull;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class FlinkJob
        implements Job
{
    private String actuatorName;
    private JobGraph jobGraph;
    private String id;  //任务的id 使用uuid
    private String description;
    private JobParameter jobParameter;
    private Flow flow;  //加载job的目录

    @NotNull
    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @NotNull
    @Override
    public String getActuatorName()
    {
        return actuatorName;
    }

    @NotNull
    @Override
    public Flow getFlow()
    {
        return flow;
    }

    @Override
    public boolean getIsOnline()
    {
        return false;
    }

    public static Builder newJob()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final FlinkJob flinkJob = new FlinkJob();

        public Builder setJobParameter(JobParameter parameter)
        {
            flinkJob.jobParameter = parameter;
            return this;
        }

        /**
         * 执行器名称
         */
        public Builder setActuatorName(final String actuatorName)
        {
            flinkJob.actuatorName = actuatorName;
            return this;
        }

        public Builder setJobGraph(JobGraph jobGraph)
        {
            flinkJob.jobGraph = jobGraph;
            return this;
        }

        public Builder setFlow(Flow flow)
        {
            flinkJob.flow = flow;
            return this;
        }

        public Builder setDescription(String description)
        {
            flinkJob.description = description;
            return this;
        }

        public Builder setId(String id)
        {
            flinkJob.id = id;
            return this;
        }

        public FlinkJob build()
                throws IOException
        {
            requireNonNull(flinkJob.actuatorName, "actuatorName is null");
            requireNonNull(flinkJob.id, "Job id is null");
            requireNonNull(flinkJob.jobGraph, "jobGraph must not null");
            return flinkJob;
        }
    }
}
