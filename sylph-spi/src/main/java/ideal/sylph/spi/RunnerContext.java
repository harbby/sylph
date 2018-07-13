package ideal.sylph.spi;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.validation.constraints.NotNull;

public interface RunnerContext
{
    @NotNull
    YarnConfiguration getYarnConfiguration();
}
