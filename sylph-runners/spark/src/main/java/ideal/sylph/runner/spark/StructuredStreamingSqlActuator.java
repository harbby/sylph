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

import com.github.harbby.gadtry.collection.mutable.MutableSet;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.jvm.JVMException;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.job.SqlFlow;
import ideal.sylph.spi.ConnectorStore;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URLClassLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static ideal.sylph.runner.spark.SQLHepler.buildSql;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

@Name("StructuredStreamingSql")
@Description("this is spark structured streaming sql Actuator")
public class StructuredStreamingSqlActuator
        extends SparkStreamingSqlActuator
{
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingSqlActuator.class);
    private final RunnerContext runnerContext;

    @Autowired
    public StructuredStreamingSqlActuator(RunnerContext runnerContext)
    {
        super(runnerContext);
        this.runnerContext = runnerContext;
    }

    @Override
    public JobHandle formJob(String jobId, Flow inFlow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws Exception
    {
        SqlFlow flow = (SqlFlow) inFlow;
        //----- compile --
        SparkJobConfig sparkJobConfig = ((SparkJobConfig.SparkConfReader) jobConfig).getConfig();
        return compile(jobId, flow, getConnectorStore(), sparkJobConfig, jobClassLoader);
    }

    @Override
    public ConnectorStore getConnectorStore()
    {
        Set<Class<?>> filterClass = MutableSet.of(
                org.apache.spark.sql.SparkSession.class,
                org.apache.spark.sql.Dataset.class,
                org.apache.spark.sql.Row.class);
        return runnerContext.createConnectorStore(filterClass, SparkRunner.class);
    }

    private static JobHandle compile(String jobId, SqlFlow sqlFlow, ConnectorStore connectorStore, SparkJobConfig sparkJobConfig, URLClassLoader jobClassLoader)
            throws JVMException
    {
        final AtomicBoolean isCompile = new AtomicBoolean(true);
        final Supplier<SparkSession> appGetter = (Supplier<SparkSession> & JobHandle & Serializable) () -> {
            logger.info("========create spark StreamingContext mode isCompile = " + isCompile.get() + "============");
            SparkConf sparkConf = isCompile.get() ?
                    new SparkConf().setMaster("local[*]").setAppName("sparkCompile")
                    : new SparkConf();
            SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

            //build sql
            SqlAnalyse sqlAnalyse = new StructuredStreamingSqlAnalyse(sparkSession, connectorStore, isCompile.get());
            try {
                buildSql(sqlAnalyse, jobId, sqlFlow);
            }
            catch (Exception e) {
                throwsException(e);
            }
            return sparkSession;
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
}
