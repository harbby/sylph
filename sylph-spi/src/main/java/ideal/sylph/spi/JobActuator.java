package ideal.sylph.spi;

import javax.annotation.Nonnull;

import java.io.File;

public interface JobActuator
{
    @Nonnull
    default Job formJob(File jobDir)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    default void execJob(@Nonnull Job job)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
