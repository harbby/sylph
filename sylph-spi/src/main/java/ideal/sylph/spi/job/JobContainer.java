package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

/**
 * Job Container
 */
public interface JobContainer
{
    @NotNull
    String getRunId();

    public void run();

    public void shutdown();

    @NotNull
    Job.Status getStatus();

    /**
     * get app run web url
     */
    String getJobUrl();
}
