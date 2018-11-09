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

import ideal.common.ioc.Binds;
import ideal.common.jvm.JVMException;
import ideal.common.jvm.JVMLauncher;
import ideal.common.jvm.JVMLaunchers;
import ideal.sylph.runner.spark.etl.sparkstreaming.StreamNodeLoader;
import ideal.sylph.runner.spark.etl.structured.StructuredNodeLoader;
import ideal.sylph.spi.App;
import ideal.sylph.spi.GraphApp;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

/**
 * SparkJobHandle 会在yarn集群中 进行序列化在具体位置在{@link ideal.sylph.runner.spark.SparkAppMain#main}这个函数中
 * 因此这个工具类 目的是 减少SparkJobHandle 序列化时的依赖,
 * SparkJobHandle序列化则 只需要依赖上面import导入class ,最核心的一点是移除了guava和其他无关依赖
 */
final class JobHelper
{
    private JobHelper() {}

    private static final Logger logger = LoggerFactory.getLogger(JobHelper.class);

    static SparkJobHandle<App<SparkSession>> build2xJob(String jobId, EtlFlow flow, URLClassLoader jobClassLoader, PipelinePluginManager pluginManager)
    {
        final AtomicBoolean isCompile = new AtomicBoolean(true);
        Supplier<App<SparkSession>> appGetter = (Supplier<App<SparkSession>> & Serializable) () -> new GraphApp<SparkSession, Dataset<Row>>()
        {
            private final SparkSession spark = getSparkSession();

            private SparkSession getSparkSession()
            {
                logger.info("========create spark SparkSession mode isCompile = " + isCompile.get() + "============");
                return isCompile.get() ? SparkSession.builder()
                        .appName("sparkCompile")
                        .master("local[*]")
                        .getOrCreate()
                        : SparkSession.builder().getOrCreate();
            }

            @Override
            public NodeLoader<Dataset<Row>> getNodeLoader()
            {
                Binds binds = Binds.builder()
                        .bind(SparkSession.class, spark)
                        .build();
                return new StructuredNodeLoader(pluginManager, binds)
                {
                    @Override
                    public UnaryOperator<Dataset<Row>> loadSink(String driverStr, Map<String, Object> config)
                    {
                        return isCompile.get() ? (stream) -> {
                            super.loadSinkWithComplic(driverStr, config).apply(stream);
                            return null;
                        } : super.loadSink(driverStr, config);
                    }
                };
            }

            @Override
            public SparkSession getContext()
            {
                return spark;
            }

            @Override
            public void build()
                    throws Exception
            {
                this.buildGraph(jobId, flow).run();
            }
        };

        try {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setCallable(() -> {
                        appGetter.get().build();
                        return 1;
                    })
                    .setConsole((line) -> System.out.println(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                    .addUserURLClassLoader(jobClassLoader)
                    .notDepThisJvmClassPath()
                    .build();
            launcher.startAndGet(jobClassLoader);
            isCompile.set(false);
            return new SparkJobHandle<>(appGetter);
        }
        catch (IOException | ClassNotFoundException | JVMException e) {
            throw new SylphException(JOB_BUILD_ERROR, "JOB_BUILD_ERROR", e);
        }
    }

    static SparkJobHandle<App<StreamingContext>> build1xJob(String jobId, EtlFlow flow, URLClassLoader jobClassLoader, PipelinePluginManager pluginManager)
    {
        final AtomicBoolean isCompile = new AtomicBoolean(true);
        final Supplier<App<StreamingContext>> appGetter = (Supplier<App<StreamingContext>> & Serializable) () -> new GraphApp<StreamingContext, DStream<Row>>()
        {
            private final StreamingContext spark = getStreamingContext();

            private StreamingContext getStreamingContext()
            {
                logger.info("========create spark StreamingContext mode isCompile = " + isCompile.get() + "============");
                SparkConf sparkConf = isCompile.get() ?
                        new SparkConf().setMaster("local[*]").setAppName("sparkCompile")
                        : new SparkConf();
                //todo: 5s is default
                return new StreamingContext(sparkConf, Seconds.apply(5));
            }

            @Override
            public NodeLoader<DStream<Row>> getNodeLoader()
            {
                Binds binds = Binds.builder()
                        .bind(StreamingContext.class, spark)
                        .build();
                return new StreamNodeLoader(pluginManager, binds);
            }

            @Override
            public StreamingContext getContext()
            {
                return spark;
            }

            @Override
            public void build()
                    throws Exception
            {
                this.buildGraph(jobId, flow).run();
            }
        };

        try {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setCallable(() -> {
                        appGetter.get().build();
                        return 1;
                    })
                    .setConsole((line) -> System.out.println(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                    .addUserURLClassLoader(jobClassLoader)
                    .notDepThisJvmClassPath()
                    .build();
            launcher.startAndGet(jobClassLoader);
            isCompile.set(false);
            return new SparkJobHandle<>(appGetter);
        }
        catch (IOException | ClassNotFoundException | JVMException e) {
            throw new SylphException(JOB_BUILD_ERROR, "JOB_BUILD_ERROR", e);
        }
    }
}
