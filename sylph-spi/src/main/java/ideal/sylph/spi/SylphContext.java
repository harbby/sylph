package ideal.sylph.spi;

import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;

import javax.validation.constraints.NotNull;

import java.util.Collection;
import java.util.Optional;

public interface SylphContext
{
    void saveJob(@NotNull String jobId, @NotNull String flow, @NotNull String actuatorName)
            throws Exception;

    void stopJob(@NotNull String jobId);

    void startJob(@NotNull String jobId);

    void deleteJob(@NotNull String jobId);

    @NotNull
    Collection<Job> getAllJobs();

    Optional<Job> getJob(String jobId);

    Optional<JobContainer> getJobContainer(@NotNull String jobId);

    Optional<JobContainer> getJobContainerWithRunId(@NotNull String jobId);

    /**
     * get all Actuator Names
     */
    Collection<JobActuator.ActuatorInfo> getAllActuatorsInfo();
}
