package ideal.sylph.runner.flink;

import com.google.inject.Binder;
import com.google.inject.Module;
import ideal.sylph.runner.flink.yarn.YarnClusterConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ideal.sylph.runner.flink.FlinkRunner.FLINK_DIST;
import static java.util.Objects.requireNonNull;

public class FlinkRunnerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        YarnConfiguration yarnConfiguration = loadYarnConfiguration();

        binder.bind(YarnConfiguration.class).toInstance(yarnConfiguration);
        binder.bind(YarnClient.class).toInstance(createYarnClient(yarnConfiguration));
        binder.bind(YarnClusterConfiguration.class).toInstance(loadYarnCluster(yarnConfiguration));
    }

    private static YarnConfiguration loadYarnConfiguration()
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Stream.of("yarn-site.xml", "core-site.xml").forEach(file -> {
            File site = new File(System.getenv("HADOOP_CONF_DIR"), file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            }
        });

        YarnConfiguration yarnConf = new YarnConfiguration(hadoopConf);
        //        try (PrintWriter pw = new PrintWriter(new FileWriter(yarnSite))) { //写到本地
//            yarnConf.writeXml(pw);
//        }
        return yarnConf;
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
                resourcesToLocalize);
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
}
