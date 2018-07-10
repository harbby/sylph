package ideal.sylph.runner.flink;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import ideal.sylph.runner.flink.runtime.StreamSqlActuator;
import ideal.sylph.spi.JobActuator;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.bootstrap.Bootstrap;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.utils.DirClassLoader;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static ideal.sylph.spi.exception.StandardErrorCode.CONFIG_ERROR;
import static java.util.Objects.requireNonNull;

public class FlinkRunner
        implements Runner
{
    public static final String FLINK_DIST = "flink-dist";

    @Override
    public Set<JobActuator> create(RunnerContext context)
    {
        requireNonNull(context, "context is null");
        final File flinkJarFile = getFlinkJarFile();

        final ClassLoader classLoader = FlinkRunner.class.getClass().getClassLoader();
        if (classLoader instanceof DirClassLoader) {
            try {
                ((DirClassLoader) classLoader).addJarFile(flinkJarFile);
            }
            catch (MalformedURLException e) {
                throw new SylphException(CONFIG_ERROR, e);
            }
        }

        try {
            var app = new Bootstrap(
                    binder -> {
                        binder.bind(YarnConfiguration.class).toInstance(context.getYarnConfiguration());
                    }
            );
            Injector injector = app.strictConfig()
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();
            return ImmutableSet.of(StreamSqlActuator.class
            ).stream().map(injector::getInstance).collect(Collectors.toSet());
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static File getFlinkJarFile()
    {
        String flinkHome = requireNonNull(System.getenv("FLINK_HOME"), "FLINK_HOME env not setting");
        if (!new File(flinkHome).exists()) {
            throw new IllegalArgumentException("FLINK_HOME " + flinkHome + " not exists");
        }
        String errorMessage = "error not search " + FLINK_DIST + "*.jar";
        File[] files = requireNonNull(new File(flinkHome, "lib").listFiles(), errorMessage);
        Optional<File> file = Arrays.stream(files)
                .filter(f -> f.getName().startsWith(FLINK_DIST)).findFirst();
        return file.orElseThrow(() -> new IllegalArgumentException(errorMessage));
    }
}
