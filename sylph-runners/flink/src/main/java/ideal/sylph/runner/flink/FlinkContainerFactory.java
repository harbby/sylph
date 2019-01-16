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

import com.github.harbby.gadtry.base.Lazys;
import com.github.harbby.gadtry.function.Creator;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.runner.flink.yarn.YarnClusterConfiguration;
import ideal.sylph.runtime.local.LocalContainer;
import ideal.sylph.runtime.yarn.YarnJobContainer;
import ideal.sylph.runtime.yarn.YarnModule;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ideal.sylph.runner.flink.FlinkRunner.FLINK_DIST;
import static ideal.sylph.runner.flink.local.MiniExec.getLocalRunner;
import static java.util.Objects.requireNonNull;

public class FlinkContainerFactory
        implements ContainerFactory
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkContainerFactory.class);

    private final Supplier<FlinkYarnJobLauncher> yarnLauncher = Lazys.goLazy(() -> {
        IocFactory injector = IocFactory.create(new YarnModule(), binder -> {
            binder.bind(FlinkYarnJobLauncher.class).withSingle();
            binder.bind(YarnClusterConfiguration.class).byCreator(FlinkContainerFactory.YarnClusterConfigurationProvider.class).withSingle();
        });
        return injector.getInstance(FlinkYarnJobLauncher.class);
    });

    @Override
    public JobContainer getYarnContainer(Job job, String lastRunid)
    {
        FlinkYarnJobLauncher jobLauncher = yarnLauncher.get();

        return YarnJobContainer.of(jobLauncher.getYarnClient(), lastRunid, () -> jobLauncher.start(job));
    }

    @Override
    public JobContainer getLocalContainer(Job job, String lastRunid)
    {
        FlinkJobHandle jobHandle = (FlinkJobHandle) job.getJobHandle();
        JobGraph jobGraph = jobHandle.getJobGraph();

        JVMLaunchers.VmBuilder<Boolean> vmBuilder = JVMLaunchers.<Boolean>newJvm()
                .setCallable(getLocalRunner(jobGraph))
                .setXms("512m")
                .setXmx("512m")
                .setConsole(System.out::println)
                .notDepThisJvmClassPath()
                .addUserjars(job.getDepends());
        return new LocalContainer(vmBuilder)
        {
            @Override
            public synchronized Optional<String> run()
                    throws Exception
            {
                this.launcher = vmBuilder.setConsole(line -> {
                    String urlMark = "Web frontend listening at";
                    if (url == null && line.contains(urlMark)) {
                        url = line.split(urlMark)[1].trim();
                    }
                    System.out.println(line);
                }).build();
                return super.run();
            }
        };
    }

    @Override
    public JobContainer getK8sContainer(Job job, String lastRunid)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    private static class YarnClusterConfigurationProvider
            implements Creator<YarnClusterConfiguration>
    {
        @Autowired private YarnConfiguration yarnConf;

        @Override
        public YarnClusterConfiguration get()
        {
            Path flinkJar = new Path(getFlinkJarFile().toURI());
            @SuppressWarnings("ConstantConditions") final Set<Path> resourcesToLocalize = Stream
                    .of("conf/log4j.properties", "conf/logback.xml")   //"conf/flink-conf.yaml"
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
