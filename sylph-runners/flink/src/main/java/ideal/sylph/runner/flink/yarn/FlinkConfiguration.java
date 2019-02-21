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
package ideal.sylph.runner.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ideal.sylph.runner.flink.FlinkRunner.FLINK_DIST;
import static java.util.Objects.requireNonNull;

public class FlinkConfiguration
{
    private final String configurationDirectory;
    private final Path flinkJar;
    private final Set<Path> resourcesToLocalize;

    private final Configuration flinkConfiguration = new Configuration();

    private FlinkConfiguration(
            String configurationDirectory,
            URI flinkJar,
            Set<URI> resourcesToLocalize)
    {
        this.configurationDirectory = configurationDirectory;
        this.flinkJar = new Path(flinkJar);
        this.resourcesToLocalize = resourcesToLocalize.stream().map(Path::new).collect(Collectors.toSet());
    }

    public String getConfigurationDirectory()
    {
        return configurationDirectory;
    }

    public Configuration flinkConfiguration()
    {
        return flinkConfiguration;
    }

    public Path flinkJar()
    {
        return flinkJar;
    }

    public Set<Path> resourcesToLocalize()
    {
        return resourcesToLocalize;
    }

    public static FlinkConfiguration of()
    {
        String flinkHome = requireNonNull(System.getenv("FLINK_HOME"), "FLINK_HOME env not setting");
        if (!new File(flinkHome).exists()) {
            throw new IllegalArgumentException("FLINK_HOME " + flinkHome + " not exists");
        }
        String configurationDirectory = new File(flinkHome, "conf").getPath();
        final Set<URI> resourcesToLocalize = Stream
                .of("log4j.properties", "logback.xml")   //"conf/flink-conf.yaml"
                .map(x -> new File(configurationDirectory, x).toURI())
                .collect(Collectors.toSet());

        //String home = "hdfs:///tmp/sylph/apps";
        return new FlinkConfiguration(
                configurationDirectory,
                getFlinkJarFile(flinkHome).toURI(),
                resourcesToLocalize);
    }

    private static File getFlinkJarFile(String flinkHome)
    {
        String errorMessage = "error not search " + FLINK_DIST + "*.jar";
        File[] files = requireNonNull(new File(flinkHome, "lib").listFiles(), errorMessage);
        Optional<File> file = Arrays.stream(files)
                .filter(f -> f.getName().startsWith(FLINK_DIST)).findFirst();
        return file.orElseThrow(() -> new IllegalArgumentException(errorMessage));
    }
}
