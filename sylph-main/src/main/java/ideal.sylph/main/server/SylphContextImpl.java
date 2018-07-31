package ideal.sylph.main.server;

import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.RunnerManger;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.YamlFlow;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import static ideal.sylph.spi.exception.StandardErrorCode.SYSTEM_ERROR;
import static ideal.sylph.spi.exception.StandardErrorCode.UNKNOWN_ERROR;
import static java.util.Objects.requireNonNull;

public class SylphContextImpl
        implements SylphContext
{
    private JobManager jobManager;
    private RunnerManger runnerManger;

    SylphContextImpl(JobManager jobManager, RunnerManger runnerManger)
    {
        this.jobManager = requireNonNull(jobManager, "jobManager is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger is null");
    }

    @Override
    public void saveJob(String jobId, String flow, String actuatorName)
            throws Exception
    {
        requireNonNull(jobId, "jobId is null");
        requireNonNull(flow, "flow is null");
        requireNonNull(actuatorName, "actuatorName is null");
        Job job = runnerManger.formJobWithFlow(jobId, YamlFlow.load(flow), actuatorName);
        jobManager.saveJob(job);
    }

    @Override
    public void stopJob(String jobId)
    {
        requireNonNull(jobId, "jobId is null");
        try {
            jobManager.stopJob(jobId);
        }
        catch (Exception e) {
            throw new SylphException(UNKNOWN_ERROR, e);
        }
    }

    @Override
    public void startJob(String jobId)
    {
        jobManager.startJob(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public void deleteJob(String jobId)
    {
        try {
            jobManager.removeJob(requireNonNull(jobId, "jobId is null"));
        }
        catch (IOException e) {
            throw new SylphException(SYSTEM_ERROR, "drop job " + jobId + " is fail", e);
        }
    }

    @Override
    public Collection<Job> getAllJobs()
    {
        return jobManager.listJobs();
    }

    @Override
    public Optional<Job> getJob(String jobId)
    {
        return jobManager.getJob(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public Optional<JobContainer> getJobContainer(String jobId)
    {
        return jobManager.getJobContainer(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public Optional<JobContainer> getJobContainerWithRunId(String runId)
    {
        return jobManager.getJobContainerWithRunId(requireNonNull(runId, "runId is null"));
    }

    @Override
    public Collection<JobActuator.ActuatorInfo> getAllActuatorsInfo()
    {
        return runnerManger.getAllActuatorsInfo();
    }
}
