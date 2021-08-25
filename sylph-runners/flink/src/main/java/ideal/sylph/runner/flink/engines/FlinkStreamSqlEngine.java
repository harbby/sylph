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

import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.google.common.collect.ImmutableSet;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.OperatorType;
import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.runner.flink.FlinkRunner;
import ideal.sylph.spi.OperatorMetaData;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.SqlFlow;
import ideal.sylph.spi.model.OperatorInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

@Name("FlinkStreamSql")
@Description("this is flink stream sql etl Actuator")
public class FlinkStreamSqlEngine
        extends FlinkStreamEtlEngine
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamSqlEngine.class);
    private static final URLClassLoader classLoader = (URLClassLoader) FlinkRunner.class.getClassLoader();
    private final RunnerContext runnerContextr;

    @Autowired
    public FlinkStreamSqlEngine(RunnerContext runnerContextr)
    {
        super(runnerContextr);
        this.runnerContextr = runnerContextr;
    }

    @NotNull
    @Override
    public Flow formFlow(byte[] flowBytes)
    {
        return new SqlFlow(flowBytes);
    }

    @Override
    public List<Class<?>> keywords()
    {
        return super.keywords();
    }

    @NotNull
    @Override
    public Collection<OperatorInfo> parserFlowDepends(Flow inFlow)
    {
        SqlFlow flow = (SqlFlow) inFlow;
        ImmutableSet.Builder<OperatorInfo> builder = ImmutableSet.builder();
        AntlrSqlParser parser = new AntlrSqlParser();
        Stream.of(flow.getSqlSplit())
                .map(query -> {
                    try {
                        return parser.createStatement(query);
                    }
                    catch (Exception x) {
                        return null;
                    }
                })
                .filter(statement -> statement instanceof CreateTable)
                .forEach(statement -> {
                    CreateTable createTable = (CreateTable) statement;
                    String driverOrName = createTable.getConnector();
                    runnerContextr.getLatestMetaData(this).findConnectorInfo(driverOrName, getPipeType(createTable.getType()))
                            .ifPresent(builder::add);
                });
        return builder.build();
    }

    @Override
    public JobGraph formJob(String jobId, Flow inFlow, JobConfig jobConfig, List<URL> pluginJars)
            throws Exception
    {
        SqlFlow flow = (SqlFlow) inFlow;
        //----- compile --
        final FlinkJobConfig jobParameter = (FlinkJobConfig) jobConfig;
        return compile(jobId, runnerContextr.getLatestMetaData(this), jobParameter, flow.getSqlSplit(), pluginJars);
    }

    private static JobGraph compile(
            String jobId,
            OperatorMetaData operatorMetaData,
            FlinkJobConfig jobConfig,
            String[] sqlSplit,
            List<URL> pluginJars)
    {
        JVMLauncher<JobGraph> launcher = JVMLaunchers.<JobGraph>newJvm()
                .setConsole((line) -> System.out.print(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset().toString()))
                .task(() -> {
                    System.out.println("************ job start ***************");
                    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
                    FlinkEnvFactory.setJobConfig(execEnv, jobConfig, jobId);
                    EnvironmentSettings settings = EnvironmentSettings.newInstance()
                            .inStreamingMode()
                            //.useBlinkPlanner()
                            .useOldPlanner()
                            .build();

                    StreamTableEnvironmentImpl tableEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(execEnv, settings);
                    StreamSqlBuilder streamSqlBuilder = new StreamSqlBuilder(tableEnv, operatorMetaData, new AntlrSqlParser());
                    Arrays.stream(sqlSplit).forEach(streamSqlBuilder::buildStreamBySql);
                    StreamGraph streamGraph;
                    try {
                        streamGraph = (StreamGraph) tableEnv.getPipeline(jobId);
                    }
                    catch (IllegalStateException e) {
                        streamGraph = execEnv.getStreamGraph(jobId);
                    }

                    streamGraph.setJobName(jobId);
                    return streamGraph.getJobGraph();
                })
                //.notDepThisJvmClassPath() //todo: filter web rest jars 应避免构建作业时平台依赖混入
                .addUserJars(ImmutableList.copy(classLoader.getURLs())) //flink jars + runner jar
                .addUserJars(pluginJars)
                .setClassLoader(classLoader)
                .build();

        return launcher.startAndGet();
    }

    private static OperatorType getPipeType(CreateTable.Type type)
    {
        switch (type) {
            case BATCH:
                return OperatorType.transform;
            case SINK:
                return OperatorType.sink;
            case SOURCE:
                return OperatorType.source;
            default:
                throw new IllegalArgumentException("this type " + type + " have't support!");
        }
    }
}
