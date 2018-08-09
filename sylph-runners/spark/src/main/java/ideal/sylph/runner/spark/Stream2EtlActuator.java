package ideal.sylph.runner.spark;

import com.google.inject.Inject;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.common.jvm.JVMException;
import ideal.sylph.common.jvm.JVMLauncher;
import ideal.sylph.common.jvm.JVMLaunchers;
import ideal.sylph.common.proxy.DynamicProxy;
import ideal.sylph.runner.spark.etl.structured.StructuredPluginLoader;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.runner.spark.yarn.YarnJobContainer;
import ideal.sylph.spi.App;
import ideal.sylph.spi.GraphApp;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.classloader.ThreadContextClassLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.validation.constraints.NotNull;

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

@Name("Spark_Structured_StreamETL")
@Description("spark2.x Structured streaming StreamETL")
public class Stream2EtlActuator
        implements JobActuatorHandle
{
    @Inject private YarnClient yarnClient;
    @Inject private SparkAppLauncher appLauncher;
    @Inject private PipelinePluginManager pluginManager;

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow flow, URLClassLoader jobClassLoader)
    {
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

    private static Supplier<App<SparkSession>> buildJob(String jobId, Flow flow, URLClassLoader jobClassLoader, PipelinePluginManager pluginManager)
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
                return new StructuredPluginLoader(pluginManager)
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
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setCallable(() -> {
                        appGetter.get().build();
                        return 1;
                    })
                    .addUserURLClassLoader(jobClassLoader)
                    .build();
            launcher.startAndGet(jobClassLoader);

            isComplic.set(false);
            return appGetter;
        }
        catch (IOException | ClassNotFoundException | JVMException e) {
            throw new SylphException(JOB_BUILD_ERROR, "JOB_BUILD_ERROR", e);
        }
    }
}
