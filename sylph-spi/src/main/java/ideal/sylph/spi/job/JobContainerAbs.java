package ideal.sylph.spi.job;

import static ideal.sylph.spi.job.Job.Status.RUNNING;
import static ideal.sylph.spi.job.Job.Status.STOP;
import static java.util.Objects.requireNonNull;

public abstract class JobContainerAbs
        implements JobContainer
{
    private Job.Status status = STOP;

    @Override
    public void setStatus(Job.Status status)
    {
        this.status = requireNonNull(status, "status is null");
    }

    @Override
    public Job.Status getStatus()
    {
        if (status == RUNNING) {
            return isRunning() ? RUNNING : STOP;
        }
        return status;
    }

    public abstract boolean isRunning();
}
