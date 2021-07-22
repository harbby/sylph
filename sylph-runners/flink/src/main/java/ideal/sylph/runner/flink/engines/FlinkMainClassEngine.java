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
package ideal.sylph.runner.flink.engines;

import com.github.harbby.gadtry.aop.AopGo;
import com.github.harbby.gadtry.aop.mock.MockGoArgument;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.jvm.JVMException;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.model.ConnectorInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.fusesource.jansi.Ansi;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;

import static com.github.harbby.gadtry.aop.mock.MockGoArgument.anyString;
import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

/**
 * 通过main class 加载Job和编译
 * <p>
 * flink submit通过{@link org.apache.flink.client.program.OptimizerPlanEnvironment#getPipeline} 加载和编译
 * 具体思路是1: setAsContext(); 设置创建静态env(session)
 * 2, 反射执行 用户main()方法
 * 3, return plan JobGraph
 */
@Name("FlinkMainClass")
@Description("this is FlinkMainClassEngine Actuator")
public class FlinkMainClassEngine
        extends FlinkStreamEtlEngine
{
    @Autowired
    public FlinkMainClassEngine(RunnerContext runnerContextr)
    {
        super(runnerContextr);
    }

    @Override
    public Flow formFlow(byte[] flowBytes)
            throws IOException
    {
        return new StringFlow(flowBytes);
    }

    @Override
    public Collection<ConnectorInfo> parserFlowDepends(Flow inFlow)
            throws IOException
    {
        return Collections.emptyList();
    }

    @Override
    public Serializable formJob(String jobId, Flow flow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws Exception
    {
        return compile(jobId, (StringFlow) flow, (FlinkJobConfig) jobConfig, jobClassLoader);
    }

    private static JobGraph compile(String jobId, StringFlow flow, FlinkJobConfig jobConfig, URLClassLoader jobClassLoader)
            throws JVMException
    {
        JVMLauncher<JobGraph> launcher = JVMLaunchers.<JobGraph>newJvm()
                .setConsole((line) -> System.out.print(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                .task(() -> {
                    //---set env
                    Class<?> mainClass = Class.forName(flow.mainClass);
                    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
                    FlinkEnvFactory.setJobConfig(execEnv, jobConfig, jobId);
                    return getJobGraphForJarClass(execEnv, mainClass, new String[0]);
                })
                .setClassLoader(jobClassLoader)
                .addUserURLClassLoader(jobClassLoader)
                .build();

        return launcher.startAndGet();
    }

    private static JobGraph getJobGraphForJarClass(StreamExecutionEnvironment execEnv, Class<?> mainClass, String[] args)
            throws Exception
    {
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

    public static class StringFlow
            extends Flow
    {
        private final String mainClass;

        public StringFlow(byte[] flowBytes)
        {
            this.mainClass = new String(flowBytes, UTF_8).trim();
        }

        @Override
        public String toString()
        {
            return mainClass;
        }
    }
}
