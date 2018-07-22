package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.util.Optional;

/**
 * Job Container
 */
public interface JobContainer
{
    /**
     * 当前正在运行的app id
     */
    @NotNull
    String getRunId();

    /**
     * online job
     *
     * @return runApp id
     */
    @NotNull
    Optional<String> run()
            throws Exception;

    /**
     * offline job
     */
    void shutdown()
            throws Exception;

    /**
     * 获取job的状态
     */
    @NotNull
    Job.Status getStatus();

    void setStatus(Job.Status status);

    /**
     * get app run web url
     */
    String getJobUrl();
}
