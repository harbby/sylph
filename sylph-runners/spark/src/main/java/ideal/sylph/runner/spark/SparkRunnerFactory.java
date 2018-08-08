package ideal.sylph.runner.spark;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.sylph.common.bootstrap.Bootstrap;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.RunnerFactory;
import ideal.sylph.spi.classloader.DirClassLoader;

import java.io.File;
import java.util.Collections;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SparkRunnerFactory
        implements RunnerFactory
{
    @Override
    public Runner create(RunnerContext context)
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
                binder.bind(SparkRunner.class).in(Scopes.SINGLETON);
                binder.bind(StreamEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(Stream2EtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(SparkAppLauncher.class).in(Scopes.SINGLETON);
                binder.bind(SparkSubmitActuator.class).in(Scopes.SINGLETON);
            });
            Injector injector = app.strictConfig()
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();
            return injector.getInstance(SparkRunner.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
