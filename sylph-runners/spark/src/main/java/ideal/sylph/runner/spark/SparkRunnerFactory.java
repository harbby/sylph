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
package ideal.sylph.runner.spark;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.common.bootstrap.Bootstrap;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.RunnerFactory;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import sun.reflect.generics.tree.ClassTypeSignature;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SparkRunnerFactory
        implements RunnerFactory
{
    @Override
    public Runner create(RunnerContext context)
    {
        requireNonNull(context, "context is null");
        String sparkHome = requireNonNull(System.getenv("SPARK_HOME"), "SPARK_HOME not setting");
        checkArgument(new File(sparkHome).exists(), "SPARK_HOME " + sparkHome + " not exists");

        ClassLoader classLoader = this.getClass().getClassLoader();
        try {
            if (classLoader instanceof DirClassLoader) {
                ((DirClassLoader) classLoader).addDir(new File(sparkHome, "jars"));
            }

            Bootstrap app = new Bootstrap(new SparkRunnerModule(), binder -> {
                binder.bind(SparkRunner.class).in(Scopes.SINGLETON);
                binder.bind(StreamEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(Stream2EtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(SparkAppLauncher.class).in(Scopes.SINGLETON);
                binder.bind(SparkSubmitActuator.class).in(Scopes.SINGLETON);
                //------------------------
                binder.bind(PipelinePluginManager.class)
                        .toProvider(() -> createPipelinePluginManager(context))
                        .in(Scopes.SINGLETON);
            });
            Injector injector = app.strictConfig()
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();
            return injector.getInstance(SparkRunner.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private static PipelinePluginManager createPipelinePluginManager(RunnerContext context)
    {
        Set<String> keyword = Stream.of(
                org.apache.spark.streaming.StreamingContext.class,
                org.apache.spark.streaming.dstream.DStream.class,
                org.apache.spark.sql.SparkSession.class,
                org.apache.spark.sql.Dataset.class
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
