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
package ideal.sylph.runner.spark;

import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.jvm.JVMException;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.google.common.collect.ImmutableSet;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.job.SqlFlow;
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.model.ConnectorInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.Serializable;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static ideal.sylph.runner.spark.SQLHepler.buildSql;
import static java.util.Objects.requireNonNull;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

/**
 * DStreamGraph graph = inputStream.graph();   //spark graph ?
 */
@Name("SparkStreamingSql")
@Description("this is spark streaming sql Actuator")
public class SparkStreamingSqlActuator
        extends StreamEtlActuator
{
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingSqlActuator.class);
    private final ConnectorStore connectorStore;

    @Autowired
    public SparkStreamingSqlActuator(RunnerContext runnerContext)
    {
        super(runnerContext);
        this.connectorStore = super.getConnectorStore();
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
                .map(parser::createStatement)
                .filter(statement -> statement instanceof CreateTable)
                .forEach(statement -> {
                    CreateTable createTable = (CreateTable) statement;
                    Map<String, Object> withConfig = createTable.getWithConfig();
                    String driverOrName = (String) requireNonNull(withConfig.get("type"), "driver is null");
                    connectorStore.findConnectorInfo(driverOrName, getPipeType(createTable.getType()))
                            .ifPresent(builder::add);
                });
        return builder.build();
    }

    @Override
    public Class<? extends JobConfig> getConfigParser()
    {
        return SparkJobConfig.SparkConfReader.class;
    }

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow inFlow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws Exception
    {
        SqlFlow flow = (SqlFlow) inFlow;
        //----- compile --
        SparkJobConfig sparkJobConfig = ((SparkJobConfig.SparkConfReader) jobConfig).getConfig();
        return compile(jobId, flow, connectorStore, sparkJobConfig, jobClassLoader);
    }

    private static JobHandle compile(String jobId, SqlFlow sqlFlow, ConnectorStore connectorStore, SparkJobConfig sparkJobConfig, URLClassLoader jobClassLoader)
            throws JVMException
    {
        int batchDuration = sparkJobConfig.getSparkStreamingBatchDuration();
        final AtomicBoolean isCompile = new AtomicBoolean(true);
        final Supplier<StreamingContext> appGetter = (Supplier<StreamingContext> & JobHandle & Serializable) () -> {
            logger.info("========create spark StreamingContext mode isCompile = " + isCompile.get() + "============");
            SparkConf sparkConf = isCompile.get() ?
                    new SparkConf().setMaster("local[*]").setAppName("sparkCompile")
                    : new SparkConf();
            SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            StreamingContext ssc = new StreamingContext(sparkSession.sparkContext(), Duration.apply(batchDuration));

            //build sql
            SqlAnalyse analyse = new SparkStreamingSqlAnalyse(ssc, connectorStore, isCompile.get());
            try {
                buildSql(analyse, jobId, sqlFlow);
            }
            catch (Exception e) {
                throwsException(e);
            }
            return ssc;
        };

        JVMLauncher<Boolean> launcher = JVMLaunchers.<Boolean>newJvm()
                .setConsole((line) -> System.out.println(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                .setCallable(() -> {
                    System.out.println("************ job start ***************");
                    appGetter.get();
                    return true;
                })
                .addUserURLClassLoader(jobClassLoader)
                .setClassLoader(jobClassLoader)
                .notDepThisJvmClassPath()
                .build();

        launcher.startAndGet();
        isCompile.set(false);
        return (JobHandle) appGetter;
    }

    private static PipelinePlugin.PipelineType getPipeType(CreateTable.Type type)
    {
        switch (type) {
            case BATCH:
                return PipelinePlugin.PipelineType.transform;
            case SINK:
                return PipelinePlugin.PipelineType.sink;
            case SOURCE:
                return PipelinePlugin.PipelineType.source;
            default:
                throw new IllegalArgumentException("this type " + type + " have't support!");
        }
    }
}
