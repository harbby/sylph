package ideal.sylph.runner.flink;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.sylph.common.bootstrap.Bootstrap;
import ideal.sylph.runner.flink.runtime.StreamEtlActuator;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.job.JobActuatorHandle;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class FlinkRunner
        implements Runner
{
    public static final String FLINK_DIST = "flink-dist";

    @Override
    public Set<JobActuatorHandle> create(RunnerContext context)
    {
        requireNonNull(context, "context is null");
        String flinkHome = requireNonNull(System.getenv("FLINK_HOME"), "FLINK_HOME not setting");
        checkArgument(new File(flinkHome).exists(), "FLINK_HOME " + flinkHome + " not exists");

        final ClassLoader classLoader = this.getClass().getClassLoader();
        try {
            if (classLoader instanceof DirClassLoader) {
                ((DirClassLoader) classLoader).addDir(new File(flinkHome, "lib"));
            }

            Bootstrap app = new Bootstrap(new FlinkRunnerModule(), binder -> {
                binder.bind(StreamEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(FlinkYarnJobLauncher.class).in(Scopes.SINGLETON);
            });
            Injector injector = app.strictConfig()
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();
            return ImmutableSet.of(StreamEtlActuator.class
            ).stream().map(injector::getInstance).collect(Collectors.toSet());
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
