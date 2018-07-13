package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

/**
 * Job Container
 */
public interface JobContainer
{
    @NotNull
    String getId();

    public void run();

    @NotNull
    Job.Status getStatus();
}
