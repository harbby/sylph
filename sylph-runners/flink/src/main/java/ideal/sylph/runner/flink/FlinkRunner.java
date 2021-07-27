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

import com.github.harbby.gadtry.easyspi.DirClassLoader;
import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.runner.flink.engines.FlinkMainClassEngine;
import ideal.sylph.runner.flink.engines.FlinkStreamEtlEngine;
import ideal.sylph.runner.flink.engines.FlinkStreamSqlEngine;
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.JobEngineHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.Throwables.throwsThrowable;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FlinkRunner
        implements Runner
{
    public static final String FLINK_DIST = "flink-dist";
    private static final Logger logger = LoggerFactory.getLogger(FlinkRunner.class);

    @Override
    public Class<? extends ContainerFactory> getContainerFactory()
    {
        return FlinkContainerFactory.class;
    }

    @Override
    public Set<JobEngineHandle> create(RunnerContext context)
    {
        requireNonNull(context, "context is null");
        String flinkHome = requireNonNull(System.getenv("FLINK_HOME"), "FLINK_HOME not setting");
        checkArgument(new File(flinkHome).exists(), "FLINK_HOME " + flinkHome + " not exists");

        final ClassLoader classLoader = this.getClass().getClassLoader();
        try {
            if (classLoader instanceof DirClassLoader) {
                ((DirClassLoader) classLoader).addDir(new File(flinkHome, "lib"));
            }
            IocFactory injector = IocFactory.create(binder -> {
                binder.bind(FlinkMainClassEngine.class).withSingle();
                binder.bind(FlinkStreamEtlEngine.class).withSingle();
                binder.bind(FlinkStreamSqlEngine.class).withSingle();
                binder.bind(RunnerContext.class, context);
            });

            return Stream.of(FlinkMainClassEngine.class, FlinkStreamEtlEngine.class, FlinkStreamSqlEngine.class)
                    .map(injector::getInstance).collect(Collectors.toSet());
        }
        catch (Exception e) {
            throw throwsThrowable(e);
        }
    }

    public static ConnectorStore createConnectorStore(RunnerContext context)
    {
        final Set<Class<?>> keyword = Stream.of(
                org.apache.flink.streaming.api.datastream.DataStream.class,
                org.apache.flink.types.Row.class).collect(Collectors.toSet());
        return context.createConnectorStore(keyword, FlinkRunner.class);
    }
}
