package ideal.sylph.runner.flink;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.sylph.common.bootstrap.Bootstrap;
import ideal.sylph.runner.flink.runtime.FlinkStreamEtlActuator;
import ideal.sylph.runner.flink.runtime.FlinkStreamSqlActuator;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.RunnerFactory;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class FlinkRunnerFactory
        implements RunnerFactory
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkRunnerFactory.class);

    @Override
    public Runner create(RunnerContext context)
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
                binder.bind(FlinkRunner.class).in(Scopes.SINGLETON);
                binder.bind(FlinkStreamEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(FlinkStreamSqlActuator.class).in(Scopes.SINGLETON);
                binder.bind(FlinkYarnJobLauncher.class).in(Scopes.SINGLETON);
                //----------------------------------
                binder.bind(PipelinePluginManager.class).toInstance(context.getPluginManager());
            });
            Injector injector = app.strictConfig()
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();
            return injector.getInstance(FlinkRunner.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
