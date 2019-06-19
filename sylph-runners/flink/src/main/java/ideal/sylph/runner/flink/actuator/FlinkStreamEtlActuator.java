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

import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.runner.flink.FlinkBean;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.runner.flink.FlinkJobHandle;
import ideal.sylph.runner.flink.etl.FlinkNodeLoader;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.job.EtlJobActuatorHandle;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.ConnectorStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.net.URLClassLoader;

import static com.google.common.base.MoreObjects.toStringHelper;
import static ideal.sylph.runner.flink.FlinkRunner.createConnectorStore;
import static ideal.sylph.spi.GraphAppUtil.buildGraph;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

@Name("FlinkStream")
@Description("this is stream etl Actuator")
@JobActuator.Mode(JobActuator.ModeType.STREAM_ETL)
public class FlinkStreamEtlActuator
        extends EtlJobActuatorHandle
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamEtlActuator.class);

    private final RunnerContext runnerContext;

    @Autowired
    public FlinkStreamEtlActuator(RunnerContext runnerContext)
    {
        this.runnerContext = runnerContext;
    }

    @NotNull
    @Override
    public Class<? extends JobConfig> getConfigParser()
    {
        return FlinkJobConfig.class;
    }

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow inFlow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws Exception
    {
        EtlFlow flow = (EtlFlow) inFlow;

        final JobParameter jobParameter = ((FlinkJobConfig) jobConfig).getConfig();
        JobGraph jobGraph = compile(jobId, flow, jobParameter, jobClassLoader, getConnectorStore());
        return new FlinkJobHandle(jobGraph);
    }

    @Override
    public ConnectorStore getConnectorStore()
    {
        return createConnectorStore(runnerContext);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "streamSql")
                .add("description", ".....")
                .toString();
    }

    private static JobGraph compile(String jobId, EtlFlow flow, JobParameter jobConfig, URLClassLoader jobClassLoader, ConnectorStore connectorStore)
    {
        //---- build flow----
        JVMLauncher<JobGraph> launcher = JVMLaunchers.<JobGraph>newJvm()
                .setCallable(() -> {
                    System.out.println("************ job start ***************");
                    StreamExecutionEnvironment execEnv = FlinkEnvFactory.getStreamEnv(jobConfig, jobId);
                    StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnv);
                    SourceContext sourceContext = new SourceContext() {};

                    final IocFactory iocFactory = IocFactory.create(new FlinkBean(tableEnv), binder -> {
                        binder.bind(SourceContext.class, sourceContext);
                    });
                    FlinkNodeLoader loader = new FlinkNodeLoader(connectorStore, iocFactory);
                    buildGraph(loader, flow);
                    StreamGraph streamGraph = execEnv.getStreamGraph();
                    streamGraph.setJobName(jobId);
                    return streamGraph.getJobGraph();
                })
                .setConsole((line) -> logger.info(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset().toString()))
                .addUserURLClassLoader(jobClassLoader)
                .setClassLoader(jobClassLoader)
                .build();
        JobGraph jobGraph = launcher.startAndGet();
        //setJobConfig(jobGraph, jobConfig, jobClassLoader, jobId);
        return jobGraph;
    }
}
