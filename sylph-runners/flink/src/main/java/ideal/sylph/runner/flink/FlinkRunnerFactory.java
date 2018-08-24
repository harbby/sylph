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

import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.sylph.common.bootstrap.Bootstrap;
import ideal.sylph.runner.flink.actuator.FlinkStreamEtlActuator;
import ideal.sylph.runner.flink.actuator.FlinkStreamSqlActuator;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.RunnerFactory;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.tree.ClassTypeSignature;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                binder.bind(PipelinePluginManager.class)
                        .toProvider(() -> createPipelinePluginManager(context))
                        .in(Scopes.SINGLETON);
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

    private static PipelinePluginManager createPipelinePluginManager(RunnerContext context)
    {
        Set<String> keyword = Stream.of(
                org.apache.flink.table.api.StreamTableEnvironment.class,
                org.apache.flink.table.api.java.StreamTableEnvironment.class,
                org.apache.flink.table.api.scala.StreamTableEnvironment.class,
                org.apache.flink.streaming.api.datastream.DataStream.class
        ).map(Class::getName).collect(Collectors.toSet());

        Set<PipelinePluginManager.PipelinePluginInfo> flinkPlugin = context.getFindPlugins().stream().filter(it -> {
            if (it.getRealTime()) {
                return true;
            }
            if (it.getJavaGenerics().length == 0) {
                return false;
            }
            ClassTypeSignature typeSignature = (ClassTypeSignature) it.getJavaGenerics()[0];
            String typeName = typeSignature.getPath().get(0).getName();
            return keyword.contains(typeName);
        }).collect(Collectors.toSet());
        return new PipelinePluginManager()
        {
            @Override
            public Set<PipelinePluginInfo> getAllPlugins()
            {
                return flinkPlugin;
            }
        };
    }
}
