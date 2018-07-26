package ideal.sylph.runner.spark;

import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.model.NodeInfo;

import java.util.Optional;

@Name("SparkSubmit")
@Description("spark submit job")
public class SparkSubmitActuator
        implements JobActuator
{
    @Override
    public Job formJob(String jobId, Flow flow)
    {
        NodeInfo node = flow.getNodes().get(0);
        String cmd = node.getNodeData();
        return new Job()
        {
            @Override
            public String getDescription()
            {
                return "this is spark submit job";
            }

            @Override
            public String getId()
            {
                return jobId;
            }

            @Override
            public String getActuatorName()
            {
                return "SparkSubmit";
            }

            @Override
            public Flow getFlow()
            {
                return flow;
            }
        };
    }

    @Override
    public JobContainer createJobContainer(Job job, Optional<String> jobInfo)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
