package ideal.sylph.runner.spark;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.sylph.common.bootstrap.Bootstrap;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.job.JobActuator;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SparkRunner
        implements Runner
{
    @Override
    public Set<JobActuator> create(RunnerContext context)
    {
        requireNonNull(context, "context is null");
        String sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome == null || !new File(sparkHome).exists()) {
            throw new IllegalArgumentException("SPARK_HOME not setting");
        }

        ClassLoader classLoader = this.getClass().getClassLoader();
        try {
            if (classLoader instanceof DirClassLoader) {
                ((DirClassLoader) classLoader).addDir(new File(sparkHome, "jars"));
            }

            Bootstrap app = new Bootstrap(new SparkRunnerModule(), binder -> {
                binder.bind(StreamEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(Stream2EtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(SparkAppLauncher.class).in(Scopes.SINGLETON);
                binder.bind(SparkSubmitActuator.class).in(Scopes.SINGLETON);
            });
            Injector injector = app.strictConfig()
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();
            return ImmutableSet.of(StreamEtlActuator.class,
                    Stream2EtlActuator.class, SparkSubmitActuator.class
            ).stream().map(injector::getInstance).collect(Collectors.toSet());
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
