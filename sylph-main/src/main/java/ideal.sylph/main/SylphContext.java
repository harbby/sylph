package ideal.sylph.main;

import com.google.inject.Inject;
import ideal.sylph.main.service.JobManager;

import static java.util.Objects.requireNonNull;

public class SylphContext
{
    private final JobManager jobManager;

    @Inject
    public SylphContext(
            JobManager jobManager)
    {
        this.jobManager = requireNonNull(jobManager, "jobManager is null");
    }
}
