package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.util.Optional;

public interface JobActuator
{
    @NotNull
    default JobHandle formJob(String jobId, Flow flow)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    default JobContainer createJobContainer(@NotNull Job job, Optional<String> jobInfo)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    default ActuatorInfo getInfo()
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    interface ActuatorInfo
    {
        String[] getName();

        String getDescription();

        long getCreateTime();

        String getVersion();
    }
}
