package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.net.URLClassLoader;

public interface JobActuatorHandle
{
    @NotNull
    default JobHandle formJob(String jobId, Flow flow, URLClassLoader jobClassLoader)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    default Flow formFlow(byte[] flowString)
            throws IOException
    {
        return YamlFlow.load(flowString);
    }

    default JobContainer createJobContainer(@NotNull Job job, String jobInfo)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
