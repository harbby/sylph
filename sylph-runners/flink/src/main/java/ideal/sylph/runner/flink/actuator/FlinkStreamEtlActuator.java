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

import com.google.inject.Inject;
import ideal.common.classloader.ThreadContextClassLoader;
import ideal.common.jvm.JVMException;
import ideal.common.jvm.JVMLauncher;
import ideal.common.jvm.JVMLaunchers;
import ideal.common.jvm.VmFuture;
import ideal.common.proxy.DynamicProxy;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.runner.flink.FlinkJobHandle;
import ideal.sylph.runner.flink.etl.FlinkNodeLoader;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.spi.App;
import ideal.sylph.spi.Binds;
import ideal.sylph.spi.GraphApp;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.job.EtlJobActuatorHandle;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

@Name("StreamETL")
@Description("this is stream etl Actuator")
@JobActuator.Mode(JobActuator.ModeType.STREAM_ETL)
public class FlinkStreamEtlActuator
        extends EtlJobActuatorHandle
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamEtlActuator.class);
    @Inject private FlinkYarnJobLauncher jobLauncher;
    @Inject private PipelinePluginManager pluginManager;

    @NotNull
    @Override
    public Class<? extends JobConfig> getConfigParser()
            throws IOException
    {
        return FlinkJobConfig.class;
    }

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow inFlow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws IOException
    {
        EtlFlow flow = (EtlFlow) inFlow;

        final int parallelism = ((FlinkJobConfig) jobConfig).getConfig().getParallelism();
        JobGraph jobGraph = compile(jobId, flow, parallelism, jobClassLoader, pluginManager);
        return new FlinkJobHandle(jobGraph);
    }

    @Override
    public JobContainer createJobContainer(@NotNull Job job, String jobInfo)
    {
        JobContainer yarnJobContainer = new YarnJobContainer(jobLauncher.getYarnClient(), jobInfo)
        {
            @Override
            public Optional<String> run()
                    throws Exception
            {
                ApplicationId yarnAppId = jobLauncher.createApplication();
                this.setYarnAppId(yarnAppId);
                logger.info("Instantiating flinkSqlJob {} at yarnId {}", job.getId(), yarnAppId);
                jobLauncher.start(job, yarnAppId);
                return Optional.of(yarnAppId.toString());
            }
        };
        //----create JobContainer Proxy
        DynamicProxy invocationHandler = new DynamicProxy(yarnJobContainer)
        {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
                    throws Throwable
            {
                /*
                 * 通过这个 修改当前YarnClient的ClassLoader的为当前sdk的加载器
                 * 默认hadoop Configuration使用jvm的AppLoader,会出现 akka.version not setting的错误 原因是找不到akka相关jar包
                 * 原因是hadoop Configuration 初始化: this.classLoader = Thread.currentThread().getContextClassLoader();
                 * */
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(this.getClass().getClassLoader())) {
                    return method.invoke(yarnJobContainer, args);
                }
            }
        };

        return (JobContainer) invocationHandler.getProxy(JobContainer.class);
    }

    @Override
    public PipelinePluginManager getPluginManager()
    {
        return pluginManager;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "streamSql")
                .add("description", ".....")
                .toString();
    }

    private static JobGraph compile(String jobId, EtlFlow flow, int parallelism, URLClassLoader jobClassLoader, PipelinePluginManager pluginManager)
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
                        public NodeLoader<DataStream<Row>> getNodeLoader()
                        {
                            Binds binds = Binds.builder()
                                    .put(org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.class, execEnv)
                                    .put(org.apache.flink.table.api.StreamTableEnvironment.class, tableEnv)
                                    .put(org.apache.flink.table.api.java.StreamTableEnvironment.class, tableEnv)
                                    //.put(org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.class, null) // execEnv
                                    //.put(org.apache.flink.table.api.scala.StreamTableEnvironment.class, null)  // tableEnv
                                    .build();
                            return new FlinkNodeLoader(pluginManager, binds);
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
                .setConsole((line) -> System.out.println(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                .addUserURLClassLoader(jobClassLoader)
                .build();

        try {
            VmFuture<JobGraph> result = launcher.startAndGet(jobClassLoader);
            return result.get().orElseThrow(() -> new SylphException(JOB_BUILD_ERROR, result.getOnFailure()));
        }
        catch (IOException | ClassNotFoundException | JVMException e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }
    }
}
