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

import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import ideal.sylph.runner.spark.sparkstreaming.StreamNodeLoader;
import ideal.sylph.runner.spark.structured.StructuredNodeLoader;
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.job.EtlFlow;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static ideal.sylph.spi.GraphAppUtil.buildGraph;
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

    static Serializable build2xJob(String jobId, EtlFlow flow, URLClassLoader jobClassLoader, ConnectorStore connectorStore)
            throws Exception
    {
        final AtomicBoolean isCompile = new AtomicBoolean(true);
        Supplier<SparkSession> appGetter = (Supplier<SparkSession> & Serializable) () -> {
            logger.info("========create spark SparkSession mode isCompile = " + isCompile.get() + "============");
            SparkSession spark = isCompile.get() ? SparkSession.builder()
                    .appName("sparkCompile")
                    .master("local[*]")
                    .getOrCreate()
                    : SparkSession.builder().getOrCreate();

            IocFactory iocFactory = IocFactory.create(binder -> binder.bind(SparkSession.class, spark));
            StructuredNodeLoader loader = new StructuredNodeLoader(connectorStore, iocFactory)
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
            buildGraph(loader, flow);
            return spark;
        };

        JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                .task(() -> {
                    appGetter.get();
                    return 1;
                })
                .setConsole((line) -> System.out.print(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                .addUserURLClassLoader(jobClassLoader)
                .notDepThisJvmClassPath()
                .setClassLoader(jobClassLoader)
                .build();
        launcher.startAndGet();
        isCompile.set(false);
        return (Serializable) appGetter;
    }

    static Serializable build1xJob(String jobId, EtlFlow flow, URLClassLoader jobClassLoader, ConnectorStore connectorStore)
            throws Exception
    {
        final AtomicBoolean isCompile = new AtomicBoolean(true);
        final Supplier<StreamingContext> appGetter = (Supplier<StreamingContext> & Serializable) () -> {
            logger.info("========create spark StreamingContext mode isCompile = " + isCompile.get() + "============");
            SparkConf sparkConf = isCompile.get() ?
                    new SparkConf().setMaster("local[*]").setAppName("sparkCompile")
                    : new SparkConf();
            //todo: 5s is default
            SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            StreamingContext spark = new StreamingContext(sparkSession.sparkContext(), Seconds.apply(5));

            Bean bean = binder -> binder.bind(StreamingContext.class, spark);
            StreamNodeLoader loader = new StreamNodeLoader(connectorStore, IocFactory.create(bean));
            buildGraph(loader, flow);
            return spark;
        };

        JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                .task(() -> {
                    appGetter.get();
                    return 1;
                })
                .setConsole((line) -> System.out.print(new Ansi().fg(YELLOW).a("[" + jobId + "] ").fg(GREEN).a(line).reset()))
                .addUserURLClassLoader(jobClassLoader)
                .notDepThisJvmClassPath()
                .setClassLoader(jobClassLoader)
                .build();
        launcher.startAndGet();
        isCompile.set(false);
        return (Serializable) appGetter;
    }
}
