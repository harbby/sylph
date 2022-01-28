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
package com.github.harbby.sylph.runner.flink.engines;

import com.github.harbby.gadtry.aop.AopGo;
import com.github.harbby.gadtry.aop.mockgo.MockGoArgument;
import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.runner.flink.FlinkJobConfig;
import com.github.harbby.sylph.runner.flink.FlinkRunner;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobEngine;
import com.github.harbby.sylph.spi.job.JobParser;
import com.github.harbby.sylph.spi.job.MainClassJobParser;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;

import static com.github.harbby.gadtry.aop.mockgo.MockGoArgument.anyString;
import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * 通过main class 加载Job和编译
 * <p>
 * flink submit通过{@link org.apache.flink.client.program.OptimizerPlanEnvironment#getPipeline} 加载和编译
 * 具体思路是1: setAsContext(); 设置创建静态env(session)
 * 2, 反射执行 用户main()方法
 * 3, return plan ideal.sylph.spi.JobGraph
 */
@Name("FlinkMainClass")
@Description("this is FlinkMainClassEngine Actuator")
public class FlinkMainClassEngine
        implements JobEngine
{
    private static final URLClassLoader classLoader = (URLClassLoader) FlinkRunner.class.getClassLoader();

    @Override
    public JobParser analyze(String flowBytes)
            throws IOException
    {
        return new MainClassJobParser(flowBytes);
    }

    @Override
    public List<Class<?>> keywords()
    {
        return Collections.emptyList();
    }

    @Override
    public JobGraph compileJob(JobParser jobParser, JobConfig jobConfig)
            throws Exception
    {
        MainClassJobParser mainClassJobParser = (MainClassJobParser) jobParser;
        //---set env
        Class<?> mainClass = Class.forName(mainClassJobParser.getMainClass());
        String[] args = mainClassJobParser.getArgs();
        StreamExecutionEnvironment execEnv = FlinkEnvFactory.createFlinkEnv((FlinkJobConfig) jobConfig);
        final StreamExecutionEnvironment mock = AopGo.proxy(StreamExecutionEnvironment.class)
                .byInstance(execEnv)
                .aop(binder -> {
                    binder.doAround(x -> null).when().execute(anyString());
                    binder.doAround(x -> null).when().execute((MockGoArgument.<StreamGraph>any()));
                }).build();

        StreamExecutionEnvironmentFactory streamFactory = (configuration) -> mock;
        Method method = StreamExecutionEnvironment.class.getDeclaredMethod("initializeContextEnvironment", StreamExecutionEnvironmentFactory.class);
        method.setAccessible(true);
        method.invoke(null, streamFactory);

        Method main = mainClass.getMethod("main", String[].class);
        checkState(Modifier.isStatic(main.getModifiers()));
        main.invoke(null, (Object) args);
        return mock.getStreamGraph().getJobGraph();
    }
}
