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

import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.google.common.collect.ImmutableSet;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.Operator;
import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.runner.flink.FlinkJobConfig;
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.SqlFlow;
import ideal.sylph.spi.model.ConnectorInfo;
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

import java.io.Serializable;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

@Name("FlinkStreamSql")
@Description("this is flink stream sql etl Actuator")
public class FlinkStreamSqlEngine
        extends FlinkStreamEtlEngine
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamSqlEngine.class);
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

    @NotNull
    @Override
    public Collection<ConnectorInfo> parserFlowDepends(Flow inFlow)
    {
        SqlFlow flow = (SqlFlow) inFlow;
        ImmutableSet.Builder<ConnectorInfo> builder = ImmutableSet.builder();
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
                    getConnectorStore().findConnectorInfo(driverOrName, getPipeType(createTable.getType()))
                            .ifPresent(builder::add);
                });
        return builder.build();
    }

    @NotNull
    @Override
    public Serializable formJob(String jobId, Flow inFlow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws Exception
    {
        SqlFlow flow = (SqlFlow) inFlow;
        //----- compile --
        final FlinkJobConfig jobParameter = (FlinkJobConfig) jobConfig;
        return compile(jobId, getConnectorStore(), jobParameter, flow.getSqlSplit(), jobClassLoader);
    }

    private static JobGraph compile(
            String jobId,
            ConnectorStore connectorStore,
            FlinkJobConfig jobConfig,
            String[] sqlSplit,
            URLClassLoader jobClassLoader)
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
                    StreamSqlBuilder streamSqlBuilder = new StreamSqlBuilder(tableEnv, connectorStore, new AntlrSqlParser());
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
                .addUserURLClassLoader(jobClassLoader)
                .setClassLoader(jobClassLoader)
                .build();

        return launcher.startAndGet();
    }

    private static Operator.PipelineType getPipeType(CreateTable.Type type)
    {
        switch (type) {
            case BATCH:
                return Operator.PipelineType.transform;
            case SINK:
                return Operator.PipelineType.sink;
            case SOURCE:
                return Operator.PipelineType.source;
            default:
                throw new IllegalArgumentException("this type " + type + " have't support!");
        }
    }
}
