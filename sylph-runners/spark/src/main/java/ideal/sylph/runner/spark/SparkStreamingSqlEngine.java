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

import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.jvm.JVMException;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.google.common.collect.ImmutableSet;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.OperatorType;
import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.spi.OperatorMetaData;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.SqlFlow;
import ideal.sylph.spi.model.OperatorInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.Throwables.throwsThrowable;
import static ideal.sylph.runner.spark.SQLHepler.buildSql;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

/**
 * DStreamGraph graph = inputStream.graph();   //spark graph ?
 */
@Name("SparkStreamingSql")
@Description("this is spark streaming sql Actuator")
public class SparkStreamingSqlEngine
        extends StreamEtlEngine
{
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingSqlEngine.class);
    private static final URLClassLoader classLoader = (URLClassLoader) SparkRunner.class.getClassLoader();
    private final RunnerContext runnerContext;

    @Autowired
    public SparkStreamingSqlEngine(RunnerContext runnerContext)
    {
        super(runnerContext);
        this.runnerContext = runnerContext;
    }

    @NotNull
    @Override
    public Flow formFlow(byte[] flowBytes)
    {
        return new SqlFlow(flowBytes);
    }

    @NotNull
    @Override
    public Collection<OperatorInfo> parserFlowDepends(Flow inFlow)
    {
        SqlFlow flow = (SqlFlow) inFlow;
        ImmutableSet.Builder<OperatorInfo> builder = ImmutableSet.builder();
        AntlrSqlParser parser = new AntlrSqlParser();

        Stream.of(flow.getSqlSplit())
                .map(parser::createStatement)
                .filter(statement -> statement instanceof CreateTable)
                .forEach(statement -> {
                    CreateTable createTable = (CreateTable) statement;
                    String driverOrName = createTable.getConnector();
                    runnerContext.getLatestMetaData(this).findConnectorInfo(driverOrName, getPipeType(createTable.getType()))
                            .ifPresent(builder::add);
                });
        return builder.build();
    }

    @Override
    public Class<? extends JobConfig> getConfigParser()
    {
        return SparkJobConfig.class;
    }

    @Override
    public Serializable formJob(String jobId, Flow inFlow, JobConfig jobConfig, List<URL> pluginJars)
            throws Exception
    {
        SqlFlow flow = (SqlFlow) inFlow;
        //----- compile --
        SparkJobConfig sparkJobConfig = (SparkJobConfig) jobConfig;
        return compile(jobId, flow, runnerContext.getLatestMetaData(this), sparkJobConfig, pluginJars);
    }

    private static Serializable compile(String jobId, SqlFlow sqlFlow, OperatorMetaData operatorMetaData, SparkJobConfig sparkJobConfig, List<URL> pluginJars)
            throws JVMException
    {
        int batchDuration = sparkJobConfig.getSparkStreamingBatchDuration();
        final AtomicBoolean isCompile = new AtomicBoolean(true);
        final Supplier<StreamingContext> appGetter = (Supplier<StreamingContext> & Serializable) () -> {
            logger.info("========create spark StreamingContext mode isCompile = " + isCompile.get() + "============");
            SparkConf sparkConf = isCompile.get() ?
                    new SparkConf().setMaster("local[*]").setAppName("sparkCompile")
                    : new SparkConf();
            SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            StreamingContext ssc = new StreamingContext(sparkSession.sparkContext(), Duration.apply(batchDuration));

            //build sql
            SqlAnalyse analyse = new SparkStreamingSqlAnalyse(ssc, operatorMetaData, isCompile.get());
            try {
                buildSql(analyse, jobId, sqlFlow);
            }
            catch (Exception e) {
                throwsThrowable(e);
            }
            return ssc;
        };

        JVMLauncher<Boolean> launcher = JVMLaunchers.<Boolean>newJvm()
                .setConsole((line) -> System.out.print(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset().toString()))
                .task(() -> {
                    System.out.println("************ job start ***************");
                    appGetter.get();
                    return true;
                })
                .addUserjars(ImmutableList.copy(classLoader.getURLs())) //flink jars + runner jar
                .addUserjars(pluginJars)
                .setClassLoader(classLoader)
                .notDepThisJvmClassPath()
                .build();

        launcher.startAndGet();
        isCompile.set(false);
        return (Serializable) appGetter;
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
