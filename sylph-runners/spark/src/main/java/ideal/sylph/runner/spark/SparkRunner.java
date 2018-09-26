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
import ideal.common.classloader.DirClassLoader;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.tree.ClassTypeSignature;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SparkRunner
        implements Runner
{
    private static final Logger logger = LoggerFactory.getLogger(SparkRunner.class);

    @Override
    public Set<JobActuatorHandle> create(RunnerContext context)
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
                binder.bind(StreamEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(Stream2EtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(SparkSubmitActuator.class).in(Scopes.SINGLETON);
                binder.bind(SparkAppLauncher.class).in(Scopes.SINGLETON);
                //------------------------
                binder.bind(PipelinePluginManager.class)
                        .toProvider(() -> createPipelinePluginManager(context))
                        .in(Scopes.SINGLETON);
            });
            Injector injector = app.strictConfig()
                    .name(this.getClass().getSimpleName())
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();

            return Stream.of(StreamEtlActuator.class, Stream2EtlActuator.class, SparkSubmitActuator.class)
                    .map(injector::getInstance).collect(Collectors.toSet());
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
                org.apache.spark.sql.SparkSession.class,
                org.apache.spark.streaming.dstream.DStream.class,
                org.apache.spark.sql.Dataset.class
        ).map(Class::getName).collect(Collectors.toSet());

        Set<PipelinePluginManager.PipelinePluginInfo> flinkPlugin = context.getFindPlugins().stream()
                .filter(it -> {
                    if (it.getRealTime()) {
                        return true;
                    }
                    if (it.getJavaGenerics().length == 0) {
                        return false;
                    }
                    ClassTypeSignature typeSignature = (ClassTypeSignature) it.getJavaGenerics()[0];
                    String typeName = typeSignature.getPath().get(0).getName();
                    return keyword.contains(typeName);
                })
                .collect(Collectors.groupingBy(k -> k.getPluginFile()))
                .entrySet().stream()
                .flatMap(it -> {
                    try (DirClassLoader classLoader = new DirClassLoader(SparkRunner.class.getClassLoader())) {
                        classLoader.addDir(it.getKey());
                        for (PipelinePluginManager.PipelinePluginInfo info : it.getValue()) {
                            try {
                                @SuppressWarnings("unchecked")
                                List<Map> config = PipelinePluginManager.parserDriverConfig(classLoader.loadClass(info.getDriverClass()).asSubclass(PipelinePlugin.class), classLoader);

                                Field field = PipelinePluginManager.PipelinePluginInfo.class.getDeclaredField("pluginConfig");
                                field.setAccessible(true);
                                field.set(info, config);
                            }
                            catch (Exception e) {
                                logger.warn("parser driver config failed,with {}/{}", info.getPluginFile(), info.getDriverClass(), e);
                            }
                        }
                    }
                    catch (IOException e) {
                        logger.error("Plugins {} access failed, no plugin details will be available", it.getKey(), e);
                    }
                    return it.getValue().stream();
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
