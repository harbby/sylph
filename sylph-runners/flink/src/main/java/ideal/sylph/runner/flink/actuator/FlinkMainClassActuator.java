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
package ideal.sylph.runner.flink.actuator;

import com.github.harbby.gadtry.jvm.JVMException;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.runner.flink.FlinkJobHandle;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.PipelinePluginInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.fusesource.jansi.Ansi;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

/**
 * 通过main class 加载Job和编译
 * <p>
 * flink submit通过{@link org.apache.flink.client.program.OptimizerPlanEnvironment#getOptimizedPlan} 加载和编译
 * 具体思路是1: setAsContext(); 设置创建静态env(session)
 * 2, 反射执行 用户main()方法
 * 3, return plan JobGraph
 */
@Name("FlinkMainClass")
@Description("this is FlinkMainClassActuator Actuator")
public class FlinkMainClassActuator
        extends FlinkStreamEtlActuator
{
    @Override
    public Flow formFlow(byte[] flowBytes)
            throws IOException
    {
        return new StringFlow(flowBytes);
    }

    @Override
    public Collection<PipelinePluginInfo> parserFlowDepends(Flow inFlow)
            throws IOException
    {
        return Collections.emptyList();
    }

    @Override
    public JobHandle formJob(String jobId, Flow flow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws Exception
    {
        FlinkJobConfig flinkJobConfig = (FlinkJobConfig) jobConfig;
        JobGraph jobGraph = compile(jobId, (StringFlow) flow, flinkJobConfig.getConfig(), jobClassLoader);

        return new FlinkJobHandle(jobGraph);
    }

    private static JobGraph compile(String jobId, StringFlow flow, JobParameter jobConfig, URLClassLoader jobClassLoader)
            throws JVMException
    {
        JVMLauncher<JobGraph> launcher = JVMLaunchers.<JobGraph>newJvm()
                .setConsole((line) -> System.out.println(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                .setCallable(() -> {
                    //---set env
                    Configuration configuration = new Configuration();
                    OptimizerPlanEnvironment planEnvironment = new OptimizerPlanEnvironment(new Optimizer(configuration));
                    ExecutionEnvironmentFactory factory = () -> planEnvironment;
                    Method method = ExecutionEnvironment.class.getDeclaredMethod("initializeContextEnvironment", ExecutionEnvironmentFactory.class);
                    method.setAccessible(true);
                    method.invoke(null, factory);

                    //--set streamEnv
                    StreamExecutionEnvironment streamExecutionEnvironment = FlinkEnvFactory.getStreamEnv(jobConfig, jobId);
                    StreamExecutionEnvironmentFactory streamFactory = () -> streamExecutionEnvironment;
                    Method m1 = StreamExecutionEnvironment.class.getDeclaredMethod("initializeContextEnvironment", StreamExecutionEnvironmentFactory.class);
                    m1.setAccessible(true);
                    m1.invoke(null, streamFactory);
                    //---
                    Class<?> mainClass = Class.forName(flow.mainClass);
                    System.out.println("this flink job Main class: " + mainClass);
                    Method main = mainClass.getMethod("main", String[].class);
                    try {
                        main.invoke(null, (Object) new String[0]);
                        throwsException(ProgramInvocationException.class);
                    }
                    catch (ProgramInvocationException e) {
                        throw e;
                    }
                    catch (Throwable t) {
                        Field field = OptimizerPlanEnvironment.class.getDeclaredField("optimizerPlan");
                        field.setAccessible(true);
                        FlinkPlan flinkPlan = (FlinkPlan) field.get(planEnvironment);
                        if (flinkPlan == null) {
                            throw new ProgramInvocationException("The program caused an error: ", t);
                        }
                        if (flinkPlan instanceof StreamGraph) {
                            return ((StreamGraph) flinkPlan).getJobGraph();
                        }
                        else {
                            final JobGraphGenerator jobGraphGenerator = new JobGraphGenerator(configuration);
                            return jobGraphGenerator.compileJobGraph((OptimizedPlan) flinkPlan, null);
                        }
                    }

                    throw new ProgramInvocationException("The program plan could not be fetched - the program aborted pre-maturely.");
                })
                .setClassLoader(jobClassLoader)
                .addUserURLClassLoader(jobClassLoader)
                .build();

        return launcher.startAndGet();
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
