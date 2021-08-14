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

import com.github.harbby.gadtry.base.Platform;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.TableContext;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.runner.flink.FlinkBean;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.runner.flink.FlinkRunner;
import ideal.sylph.runner.flink.etl.FlinkNodeLoader;
import ideal.sylph.spi.OperatorMetaData;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.job.EtlJobEngineHandle;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static ideal.sylph.spi.GraphAppUtil.buildGraph;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

@Name("FlinkStream")
@Description("this is stream etl Actuator")
public class FlinkStreamEtlEngine
        extends EtlJobEngineHandle
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamEtlEngine.class);
    private static final URLClassLoader classLoader = (URLClassLoader) FlinkRunner.class.getClassLoader();
    private final RunnerContext runnerContext;

    @Autowired
    public FlinkStreamEtlEngine(RunnerContext runnerContext)
    {
        super(runnerContext);
        this.runnerContext = runnerContext;
    }

    @Override
    public Class<? extends JobConfig> getConfigParser()
    {
        return FlinkJobConfig.class;
    }

    @Override
    public JobGraph formJob(String jobId, Flow inFlow, JobConfig jobConfig, List<URL> pluginJars)
            throws Exception
    {
        EtlFlow flow = (EtlFlow) inFlow;

        final FlinkJobConfig jobParameter = (FlinkJobConfig) jobConfig;
        return compile(jobId, flow, jobParameter, pluginJars, runnerContext.getLatestMetaData(this));
    }

    @Override
    public List<Class<?>> keywords()
    {
        return ImmutableList.of(
                org.apache.flink.streaming.api.datastream.DataStream.class,
                org.apache.flink.types.Row.class);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "streamSql")
                .add("description", ".....")
                .toString();
    }

    private static JobGraph compile(String jobId, EtlFlow flow, FlinkJobConfig jobConfig, List<URL> pluginJars, OperatorMetaData operatorMetaData)
    {
        //---- build flow----
        JVMLauncher<JobGraph> launcher = JVMLaunchers.<JobGraph>newJvm()
                .task(() -> {
                    System.out.println("************ job start ***************");
                    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
                    FlinkEnvFactory.setJobConfig(execEnv, jobConfig, jobId);
                    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(execEnv);
                    TableContext sourceContext = Platform.allocateInstance(TableContext.class);

                    final IocFactory iocFactory = IocFactory.create(new FlinkBean(execEnv, tableEnv), binder -> {
                        binder.bind(TableContext.class, sourceContext);
                    });
                    FlinkNodeLoader loader = new FlinkNodeLoader(operatorMetaData, iocFactory);
                    buildGraph(loader, flow);
                    StreamGraph streamGraph = execEnv.getStreamGraph();
                    streamGraph.setJobName(jobId);
                    return streamGraph.getJobGraph();
                })
                .setConsole((line) -> System.out.print(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset().toString()))
                .addUserjars(ImmutableList.copy(classLoader.getURLs())) //flink jars + runner jar
                .addUserjars(pluginJars)
                .setClassLoader(classLoader)
                .build();
        return launcher.startAndGet(); //setJobConfig(jobGraph, jobConfig, jobClassLoader, jobId);
    }
}
