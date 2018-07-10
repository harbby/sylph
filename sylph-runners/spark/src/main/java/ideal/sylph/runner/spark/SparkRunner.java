package ideal.sylph.runner.spark;

import ideal.sylph.shaded.com.google.common.collect.ImmutableSet;
import ideal.sylph.spi.JobActuator;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;

import java.util.Set;

public class SparkRunner
        implements Runner
{
    @Override
    public Set<JobActuator> create(RunnerContext context)
    {
        //throw new UnsupportedOperationException("this method have't support!");
        return ImmutableSet.of();
    }
}
