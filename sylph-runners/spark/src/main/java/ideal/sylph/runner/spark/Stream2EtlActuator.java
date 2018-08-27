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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.common.jvm.JVMException;
import ideal.common.jvm.JVMLauncher;
import ideal.common.jvm.JVMLaunchers;
import ideal.common.proxy.DynamicProxy;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.runner.spark.etl.structured.StructuredNodeLoader;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.runner.spark.yarn.YarnJobContainer;
import ideal.sylph.spi.App;
import ideal.sylph.spi.EtlFlow;
import ideal.sylph.spi.GraphApp;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.classloader.ThreadContextClassLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.NodeInfo;
import ideal.sylph.spi.model.PipelinePluginManager;
import ideal.sylph.spi.utils.GenericTypeReference;
import ideal.sylph.spi.utils.JsonTextUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static java.util.Objects.requireNonNull;

@Name("Spark_Structured_StreamETL")
@Description("spark2.x Structured streaming StreamETL")
public class Stream2EtlActuator
        implements JobActuatorHandle
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Inject private YarnClient yarnClient;
    @Inject private SparkAppLauncher appLauncher;
    @Inject private PipelinePluginManager pluginManager;

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow inFlow, DirClassLoader jobClassLoader)
            throws IOException
    {
        EtlFlow flow = (EtlFlow) inFlow;
        //---- flow parser depends ----
        ImmutableSet.Builder<File> builder = ImmutableSet.builder();
        for (NodeInfo nodeInfo : flow.getNodes()) {
            String json = JsonTextUtil.readJsonText(nodeInfo.getNodeText());
            Map<String, Object> nodeConfig = nodeInfo.getNodeConfig();
            Map<String, Object> config = MAPPER.readValue(json, new GenericTypeReference(Map.class, String.class, Object.class));
            String driverString = (String) requireNonNull(config.get("driver"), "driver is null");
            Optional<PipelinePluginManager.PipelinePluginInfo> pluginInfo = pluginManager.findPluginInfo(driverString);
            pluginInfo.ifPresent(plugin -> FileUtils.listFiles(plugin.getPluginFile(), null, true).forEach(builder::add));
        }
        jobClassLoader.addJarFiles(builder.build());

        try {
            return new SparkJobHandle<>(buildJob(jobId, flow, jobClassLoader, pluginManager));
        }
        catch (Exception e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }
    }

    @Override
    public JobContainer createJobContainer(@NotNull Job job, String jobInfo)
    {
        final JobContainer yarnJobContainer = new YarnJobContainer(yarnClient, jobInfo)
        {
            @Override
            public Optional<String> run()
                    throws Exception
            {
                ApplicationId yarnAppId = appLauncher.run(job);
                this.setYarnAppId(yarnAppId);
                return Optional.of(yarnAppId.toString());
            }
        };
        //----create JobContainer Proxy
        DynamicProxy invocationHandler = new DynamicProxy(yarnJobContainer)
        {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
                    throws Throwable
            {
                /*
                 * 通过这个 修改当前YarnClient的ClassLoader的为当前sdk的加载器
                 * 默认hadoop Configuration使用jvm的AppLoader,会出现 akka.version not setting的错误 原因是找不到akka相关jar包
                 * 原因是hadoop Configuration 初始化: this.classLoader = Thread.currentThread().getContextClassLoader();
                 * */
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(this.getClass().getClassLoader())) {
                    return method.invoke(yarnJobContainer, args);
                }
            }
        };

        return (JobContainer) invocationHandler.getProxy(JobContainer.class);
    }

    private static Supplier<App<SparkSession>> buildJob(String jobId, EtlFlow flow, URLClassLoader jobClassLoader, PipelinePluginManager pluginManager)
    {
        final AtomicBoolean isComplic = new AtomicBoolean(true);
        Supplier<App<SparkSession>> appGetter = (Supplier<App<SparkSession>> & Serializable) () -> new GraphApp<SparkSession, Dataset<Row>>()
        {
            private final SparkSession spark = getSparkSession();

            private SparkSession getSparkSession()
            {
                System.out.println("========create spark SparkSession mode isComplic = " + isComplic.get() + "============");
                return isComplic.get() ? SparkSession.builder()
                        .appName("streamLoadTest")
                        .master("local[*]")
                        .getOrCreate()
                        : SparkSession.builder().getOrCreate();
            }

            @Override
            public NodeLoader<SparkSession, Dataset<Row>> getNodeLoader()
            {
                return new StructuredNodeLoader(pluginManager)
                {
                    @Override
                    public UnaryOperator<Dataset<Row>> loadSink(Map<String, Object> config)
                    {
                        return isComplic.get() ? (stream) -> {
                            super.loadSinkWithComplic(config).apply(stream);
                            return null;
                        } : super.loadSink(config);
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
            //AnsiConsole.systemInstall();
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setCallable(() -> {
                        appGetter.get().build();
                        return 1;
                    })
                    .setConsole(System.err::println)
                    //.setConsole((line) -> System.out.println(ansi().eraseScreen().render("@|green "+line+"|@").reset()))
                    .addUserURLClassLoader(jobClassLoader)
                    .build();
            launcher.startAndGet(jobClassLoader);

            isComplic.set(false);
            return appGetter;
        }
        catch (IOException | ClassNotFoundException | JVMException e) {
            throw new SylphException(JOB_BUILD_ERROR, "JOB_BUILD_ERROR", e);
        }
        finally {
            //AnsiConsole.systemUninstall();
        }
    }
}
