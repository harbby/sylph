/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.sylph.runner.flink;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import ideal.sylph.runner.flink.yarn.YarnClusterConfiguration;
import ideal.sylph.spi.exception.SylphException;
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
import static ideal.sylph.spi.exception.StandardErrorCode.CONFIG_ERROR;
import static java.util.Objects.requireNonNull;

public class FlinkRunnerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(YarnConfiguration.class).toProvider(FlinkRunnerModule::loadYarnConfiguration).in(Scopes.SINGLETON);
        binder.bind(YarnClient.class).toProvider(YarnClientProvider.class).in(Scopes.SINGLETON);
        binder.bind(YarnClusterConfiguration.class).toProvider(YarnClusterConfigurationProvider.class).in(Scopes.SINGLETON);
    }

    private static class YarnClientProvider
            implements Provider<YarnClient>
    {
        @Inject private YarnConfiguration yarnConfiguration;

        @Override
        public YarnClient get()
        {
            YarnClient client = YarnClient.createYarnClient();
            client.init(yarnConfiguration);
            client.start();
            return client;
        }
    }

    private static class YarnClusterConfigurationProvider
            implements Provider<YarnClusterConfiguration>
    {
        @Inject private YarnConfiguration yarnConf;

        @Override
        public YarnClusterConfiguration get()
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
    }

    private static YarnConfiguration loadYarnConfiguration()
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Stream.of("yarn-site.xml", "core-site.xml", "hdfs-site.xml").forEach(file -> {
            File site = new File(requireNonNull(System.getenv("HADOOP_CONF_DIR"), "ENV HADOOP_CONF_DIR is not setting"), file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            }
            else {
                throw new SylphException(CONFIG_ERROR, site + " not exists");
            }
        });

        YarnConfiguration yarnConf = new YarnConfiguration(hadoopConf);
        //        try (PrintWriter pw = new PrintWriter(new FileWriter(yarnSite))) { //write local file
        //            yarnConf.writeXml(pw);
        //        }
        return yarnConf;
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
