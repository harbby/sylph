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

import com.google.common.collect.ImmutableSet;
import ideal.sylph.common.jvm.JVMLauncher;
import ideal.sylph.common.jvm.JVMLaunchers;
import ideal.sylph.common.jvm.VmFuture;
import ideal.sylph.runner.flink.FlinkJobHandle;
import ideal.sylph.runner.flink.etl.FlinkNodeLoader;
import ideal.sylph.spi.App;
import ideal.sylph.spi.EtlFlow;
import ideal.sylph.spi.GraphApp;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLClassLoader;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

public final class FlinkJobUtil
{
    private FlinkJobUtil() {}

    private static final Logger logger = LoggerFactory.getLogger(FlinkJobUtil.class);

    public static JobHandle createJob(String jobId, EtlFlow flow, URLClassLoader jobClassLoader, PipelinePluginManager pluginManager)
            throws Exception
    {
        //---------编译job-------------
        JobGraph jobGraph = compile(jobId, flow, 2, jobClassLoader, pluginManager);
        //----------------设置状态----------------
        JobParameter jobParameter = new JobParameter()
                .queue("default")
                .taskManagerCount(2) //-yn 注意是executer个数
                .taskManagerMemoryMb(1024) //1024mb
                .taskManagerSlots(1) // -ys
                .jobManagerMemoryMb(1024) //-yjm
                .appTags(ImmutableSet.of("demo1", "demo2"))
                .setYarnJobName(jobId);
        //getUserAdditionalJars(userJars)
        return new FlinkJobHandle(jobGraph, jobParameter);
    }

    /**
     * 对job 进行编译
     */
    private static JobGraph compile(String jobId, EtlFlow flow, int parallelism, URLClassLoader jobClassLoader, PipelinePluginManager pluginManager)
            throws Exception
    {
        //---- build flow----
        JVMLauncher<JobGraph> launcher = JVMLaunchers.<JobGraph>newJvm()
                .setCallable(() -> {
                    System.out.println("************ job start ***************");
                    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
                    execEnv.setParallelism(parallelism);
                    StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnv);
                    App<StreamTableEnvironment> app = new GraphApp<StreamTableEnvironment, DataStream<Row>>()
                    {
                        @Override
                        public NodeLoader<StreamTableEnvironment, DataStream<Row>> getNodeLoader()
                        {
                            return new FlinkNodeLoader(pluginManager);
                        }

                        @Override
                        public StreamTableEnvironment getContext()
                        {
                            return tableEnv;
                        }

                        @Override
                        public void build()
                                throws Exception
                        {
                            this.buildGraph(jobId, flow).run();
                        }
                    };
                    app.build();
                    return execEnv.getStreamGraph().getJobGraph();
                })
                .setConsole(System.err::println)
                //.setConsole((line) -> System.out.println(ansi().eraseScreen().fg(GREEN).a(line).reset() ))
                .addUserURLClassLoader(jobClassLoader)
                .build();

        VmFuture<JobGraph> result = launcher.startAndGet(jobClassLoader);
        return result.get().orElseThrow(() -> new SylphException(JOB_BUILD_ERROR, result.getOnFailure()));
    }
}
