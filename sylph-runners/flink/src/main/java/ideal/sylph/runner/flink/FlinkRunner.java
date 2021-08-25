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

import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.spi.DynamicClassLoader;
import ideal.sylph.runner.flink.engines.FlinkMainClassEngine;
import ideal.sylph.runner.flink.engines.FlinkStreamEtlEngine;
import ideal.sylph.runner.flink.engines.FlinkStreamSqlEngine;
import ideal.sylph.runner.flink.plugins.TestSource;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.JobEngineHandle;
import ideal.sylph.spi.model.OperatorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FlinkRunner
        implements Runner
{
    public static final String FLINK_DIST = "flink-dist";
    private static final Logger logger = LoggerFactory.getLogger(FlinkRunner.class);
    private final Set<JobEngineHandle> engines = new HashSet<>();

    @Override
    public Class<? extends ContainerFactory> getContainerFactory()
    {
        return FlinkContainerFactory.class;
    }

    @Override
    public Set<JobEngineHandle> getEngines()
    {
        return engines;
    }

    @Override
    public void initialize(RunnerContext context)
            throws Exception
    {
        requireNonNull(context, "context is null");
        String flinkHome = requireNonNull(System.getenv("FLINK_HOME"), "FLINK_HOME not setting");
        checkArgument(new File(flinkHome).exists(), "FLINK_HOME " + flinkHome + " not exists");

        final ClassLoader classLoader = this.getClass().getClassLoader();

        if (classLoader instanceof DynamicClassLoader) {
            ((DynamicClassLoader) classLoader).addDir(new File(flinkHome, "lib"));
        }
        IocFactory injector = IocFactory.create(binder -> {
            binder.bind(FlinkMainClassEngine.class).withSingle();
            binder.bind(FlinkStreamEtlEngine.class).withSingle();
            binder.bind(FlinkStreamSqlEngine.class).withSingle();
            binder.bind(RunnerContext.class, context);
        });
        Stream.of(FlinkMainClassEngine.class, FlinkStreamEtlEngine.class, FlinkStreamSqlEngine.class)
                .map(injector::getInstance).forEach(engines::add);
    }

    public List<OperatorInfo> getInternalOperator()
    {
        OperatorInfo operatorInfo = OperatorInfo.analyzePluginInfo(TestSource.class,
                engines.stream().map(x -> x.getClass().getName())
                        .collect(Collectors.toList()));
        return ImmutableList.of(operatorInfo);
    }
}
