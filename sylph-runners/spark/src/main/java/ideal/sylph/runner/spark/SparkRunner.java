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

import com.github.harbby.gadtry.easyspi.DirClassLoader;
import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.JobEngineHandle;

import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SparkRunner
        implements Runner
{
    private RunnerContext context;
    private Set<JobEngineHandle> engines;

    @Override
    public void initialize(RunnerContext context)
    {
        this.context = requireNonNull(context, "context is null");
        String sparkHome = requireNonNull(System.getenv("SPARK_HOME"), "SPARK_HOME not setting");
        checkArgument(new File(sparkHome).exists(), "SPARK_HOME " + sparkHome + " not exists");

        ClassLoader classLoader = this.getClass().getClassLoader();

        if (classLoader instanceof DirClassLoader) {
            ((DirClassLoader) classLoader).addDir(new File(sparkHome, "jars"));
        }

        IocFactory injector = IocFactory.create(
                binder -> {
                    binder.bind(StreamEtlEngine.class).withSingle();
                    binder.bind(Stream2EtlEngine.class).withSingle();
                    binder.bind(SparkMainClassEngine.class).withSingle();
                    binder.bind(SparkStreamingSqlEngine.class).withSingle();
                    binder.bind(StructuredStreamingSqlEngine.class).withSingle();
                    //------------------------
                    binder.bind(RunnerContext.class).byInstance(context);
                });

        this.engines = Stream.of(
                StreamEtlEngine.class,
                Stream2EtlEngine.class,
                SparkMainClassEngine.class,
                SparkStreamingSqlEngine.class,
                StructuredStreamingSqlEngine.class
        ).map(injector::getInstance).collect(Collectors.toSet());
    }

    @Override
    public Set<JobEngineHandle> getEngines()
    {
        return engines;
    }

    @Override
    public Class<? extends ContainerFactory> getContainerFactory()
    {
        return SparkContainerFactory.class;
    }
}
