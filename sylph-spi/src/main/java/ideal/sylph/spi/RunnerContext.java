package ideal.sylph.spi;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nonnull;

public interface RunnerContext
{
    @Nonnull
    YarnConfiguration getYarnConfiguration();
}
