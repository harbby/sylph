package ideal.sylph.runner.spark;

import ideal.sylph.spi.job.JobHandle;

import java.io.Serializable;
import java.util.function.Supplier;

public class SparkJobHandle<T>
        implements JobHandle, Serializable
{
    private final Supplier<T> supplier;

    SparkJobHandle(Supplier<T> supplier)
    {
        this.supplier = supplier;
    }

    public Supplier<T> getApp()
    {
        return supplier;
    }
}
