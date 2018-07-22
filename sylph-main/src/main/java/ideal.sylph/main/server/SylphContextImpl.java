package ideal.sylph.main.server;

import com.google.inject.Inject;
import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.RunnerManger;
import ideal.sylph.main.service.YamlFlow;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;

import java.util.Collection;
import java.util.Optional;

import static ideal.sylph.spi.exception.StandardErrorCode.UNKNOWN_ERROR;

public class SylphContextImpl
        implements SylphContext
{
    @Inject
    private JobManager jobManager;

    @Inject
    private RunnerManger runnerManger;

    @Override
    public void saveJob(String jobId, String flow, String actuatorName)
            throws Exception
    {
        Job job = runnerManger.formJobWithFlow(jobId, YamlFlow.load(flow), actuatorName);
        jobManager.saveJob(job);
    }

    @Override
    public void stopJob(String jobId)
    {
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
        jobManager.startJob(jobId);
    }

    @Override
    public void deleteJob(String jobId)
    {
        jobManager.removeJob(jobId);
    }

    @Override
    public Collection<Job> getAllJobs()
    {
        return jobManager.listJobs();
    }

    @Override
    public Optional<Job> getJob(String jobId)
    {
        return jobManager.getJob(jobId);
    }

    @Override
    public Optional<JobContainer> getJobContainer(String jobId)
    {
        return jobManager.getJobContainer(jobId);
    }
}
