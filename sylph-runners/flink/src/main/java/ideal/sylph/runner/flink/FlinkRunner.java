package ideal.sylph.runner.flink;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.sylph.runner.flink.runtime.StreamEtlActuator;
import ideal.sylph.runner.flink.yarn.YarnClusterConfiguration;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.bootstrap.Bootstrap;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.utils.DirClassLoader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

        final ClassLoader classLoader = this.getClass().getClassLoader();
        if (classLoader instanceof DirClassLoader) {
            try {
                ((DirClassLoader) classLoader).addJarFile(flinkJarFile);
            }
            catch (MalformedURLException e) {
                throw new SylphException(CONFIG_ERROR, e);
            }
        }

        try {
            YarnConfiguration yarnConfiguration = context.getYarnConfiguration();
            Bootstrap app = new Bootstrap(binder -> {
                binder.bind(YarnConfiguration.class).toInstance(yarnConfiguration);
                binder.bind(StreamEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(YarnClient.class).toInstance(createYarnClient(yarnConfiguration));
                binder.bind(YarnClusterConfiguration.class).toInstance(loadYarnCluster(yarnConfiguration));
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

    private static YarnClusterConfiguration loadYarnCluster(YarnConfiguration yarnConf)
    {
        Path flinkJar = new Path(getFlinkJarFile().toURI());
        @SuppressWarnings("ConstantConditions") final Set<Path> resourcesToLocalize = Stream
                .of("conf/flink-conf.yaml", "conf/log4j.properties", "conf/logback.xml")
                .map(x -> new Path(new File(System.getenv("FLINK_HOME"), x).toURI()))
                .collect(Collectors.toSet());

        String home = "hdfs:///tmp/sylph/apps";
        return new YarnClusterConfiguration(
                yarnConf,
                home,
                flinkJar,
                resourcesToLocalize,
                systemJars());
    }

    private static YarnClient createYarnClient(YarnConfiguration yarnConfiguration)
    {
        YarnClient client = YarnClient.createYarnClient();
        client.init(yarnConfiguration);
        client.start();
        return client;
    }

    private static File getFlinkJarFile()
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

    /**
     * 当前class.path里面所有的jar
     */
    private static Set<Path> systemJars()
    {
        String[] jars = System.getProperty("java.class.path")
                .split(Pattern.quote(File.pathSeparator));
        Set<Path> res = Arrays.stream(jars).map(File::new).filter(File::isFile)
                .map(x -> new Path(x.toURI())).collect(Collectors.toSet());
        //res.forEach(x -> logger.info("systemJars: {}", x));
        //logger.info("flink job systemJars size: {}", res.size());
        return res;
    }
}
