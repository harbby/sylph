package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.io.File;

public interface JobActuator
{
    @NotNull
    default Job formJob(File jobDir, Flow flow)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    default void execJob(@NotNull Job job)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
