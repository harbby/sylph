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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.common.jvm.JVMException;
import ideal.common.jvm.JVMLauncher;
import ideal.common.jvm.JVMLaunchers;
import ideal.common.jvm.VmFuture;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.parser.SqlParser;
import ideal.sylph.parser.tree.CreateStream;
import ideal.sylph.runner.flink.FlinkJobHandle;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.fusesource.jansi.Ansi;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

@Name("StreamSql")
@Description("this is flink stream sql etl Actuator")
public class FlinkStreamSqlActuator
        extends FlinkStreamEtlActuator
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Inject private FlinkYarnJobLauncher jobLauncher;
    @Inject private PipelinePluginManager pluginManager;

    @Override
    public Flow formFlow(byte[] flowBytes)
            throws IOException
    {
        return new SqlFlow(flowBytes);
    }

    public static class SqlFlow
            extends Flow
    {
        private final String flowString;

        SqlFlow(byte[] flowBytes)
        {
            this.flowString = new String(flowBytes, UTF_8);
        }

        public String getFlowString()
        {
            return flowString;
        }

        @Override
        public String toString()
        {
            return flowString;
        }
    }

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow inFlow, DirClassLoader jobClassLoader)
            throws IOException
    {
        SqlFlow flow = (SqlFlow) inFlow;
        final String sqlText = flow.getFlowString();
        ImmutableSet.Builder<File> builder = ImmutableSet.builder();
        SqlParser parser = new SqlParser();
        String[] sqlSplit = Stream.of(sqlText.split(";(?=([^\']*\'[^\']*\')*[^\']*$)"))
                .filter(StringUtils::isNotBlank).toArray(String[]::new);
        Stream.of(sqlSplit).filter(sql -> sql.toLowerCase().contains("create ") && sql.toLowerCase().contains(" table "))
                .map(parser::createStatement)
                .filter(statement -> statement instanceof CreateStream)
                .forEach(statement -> {
                    CreateStream createTable = (CreateStream) statement;
                    Map<String, String> withConfig = createTable.getProperties().stream()
                            .collect(Collectors.toMap(
                                    k -> k.getName().getValue(),
                                    v -> v.getValue().toString().replace("'", ""))
                            );
                    String driverString = requireNonNull(withConfig.get("type"), "driver is null");
                    Optional<PipelinePluginManager.PipelinePluginInfo> pluginInfo = pluginManager.findPluginInfo(driverString);
                    pluginInfo.ifPresent(plugin -> FileUtils
                            .listFiles(plugin.getPluginFile(), null, true)
                            .forEach(builder::add));
                });





        jobClassLoader.addJarFiles(builder.build());
        //----- compile --
        final int parallelism = 2;
        JobGraph jobGraph = compile(jobId, pluginManager, parallelism, sqlSplit, jobClassLoader);
        //----------------设置状态----------------
        JobParameter jobParameter = new JobParameter()
                .queue("default")
                .taskManagerCount(2) //-yn 注意是executer个数
                .taskManagerMemoryMb(1024) //1024mb
                .taskManagerSlots(1) // -ys
                .jobManagerMemoryMb(1024) //-yjm
                .appTags(ImmutableSet.of("demo1", "demo2"))
                .setYarnJobName(jobId);

        return new FlinkJobHandle(jobGraph, jobParameter);
    }

    private static JobGraph compile(
            String jobId,
            PipelinePluginManager pluginManager,
            int parallelism,
            String[] sqlSplit,
            DirClassLoader jobClassLoader)
    {
        JVMLauncher<JobGraph> launcher = JVMLaunchers.<JobGraph>newJvm()
                .setConsole((line) -> System.out.println(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                .setCallable(() -> {
                    System.out.println("************ job start ***************");
                    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
                    execEnv.setParallelism(parallelism);
                    StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnv);
                    StreamSqlBuilder streamSqlBuilder = new StreamSqlBuilder(tableEnv, pluginManager, new SqlParser());
                    Arrays.stream(sqlSplit).forEach(streamSqlBuilder::buildStreamBySql);
                    return execEnv.getStreamGraph().getJobGraph();
                })
                .addUserURLClassLoader(jobClassLoader)
                .build();

        try {
            launcher.startAndGet(jobClassLoader);
            VmFuture<JobGraph> result = launcher.startAndGet(jobClassLoader);
            return result.get().orElseThrow(() -> new SylphException(JOB_BUILD_ERROR, result.getOnFailure()));
        }
        catch (IOException | JVMException | ClassNotFoundException e) {
            throw new RuntimeException("StreamSql job build failed", e);
        }
    }
}
