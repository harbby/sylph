package ideal.sylph.runner.spark;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.NodeInfo;

import javax.validation.constraints.NotNull;

import java.net.URLClassLoader;

@Name("SparkSubmit")
@Description("spark submit job")
public class SparkSubmitActuator
        implements JobActuatorHandle
{
    @Override
    public JobHandle formJob(String jobId, Flow flow, URLClassLoader jobClassLoader)
    {
        NodeInfo node = flow.getNodes().get(0);
        String cmd = node.getNodeData();
        return new JobHandle() {};
    }

    @Override
    public JobContainer createJobContainer(@NotNull Job job, String jobInfo)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
