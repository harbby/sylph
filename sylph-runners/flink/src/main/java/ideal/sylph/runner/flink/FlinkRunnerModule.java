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
import ideal.sylph.runner.flink.actuator.FlinkStreamEtlActuator;
import ideal.sylph.runner.flink.actuator.FlinkStreamSqlActuator;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.runner.flink.yarn.YarnClusterConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(FlinkRunnerModule.class);

    @Override
    public void configure(Binder binder)
    {
        binder.bind(FlinkStreamEtlActuator.class).in(Scopes.SINGLETON);
        binder.bind(FlinkStreamSqlActuator.class).in(Scopes.SINGLETON);
        binder.bind(FlinkYarnJobLauncher.class).in(Scopes.SINGLETON);
        binder.bind(YarnClusterConfiguration.class).toProvider(YarnClusterConfigurationProvider.class).in(Scopes.SINGLETON);
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
