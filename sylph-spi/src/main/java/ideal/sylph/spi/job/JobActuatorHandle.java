package ideal.sylph.spi.job;

import ideal.sylph.spi.classloader.DirClassLoader;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.io.IOException;

public interface JobActuatorHandle
{
    @Nullable
    default JobHandle formJob(String jobId, Flow flow, DirClassLoader jobClassLoader)
            throws IOException
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    default Flow formFlow(byte[] flowBytes)
            throws IOException
    {
        return YamlFlow.load(flowBytes);
    }

    default JobContainer createJobContainer(@NotNull Job job, String jobInfo)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
