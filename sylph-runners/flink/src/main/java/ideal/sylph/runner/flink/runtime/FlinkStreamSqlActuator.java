package ideal.sylph.runner.flink.runtime;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobHandle;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.net.URLClassLoader;

@Name("StreamSql")
@Description("this is flink stream sql etl Actuator")
public class FlinkStreamSqlActuator
        extends FlinkStreamEtlActuator
{
    @Override
    public Flow formFlow(byte[] flowString)
            throws IOException
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow flow, URLClassLoader jobClassLoader)
    {
        return null;
    }
}
