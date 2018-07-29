package ideal.sylph.runner.spark;

import com.google.inject.Inject;
import ideal.sylph.common.base.Serializables;
import ideal.sylph.common.graph.Graph;
import ideal.sylph.common.jvm.JVMLauncher;
import ideal.sylph.common.jvm.JVMLaunchers;
import ideal.sylph.common.jvm.JVMRunningException;
import ideal.sylph.common.proxy.DynamicProxy;
import ideal.sylph.runner.spark.etl.structured.StructuredPluginLoader;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.runner.spark.yarn.YarnJobContainer;
import ideal.sylph.spi.App;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.classloader.ThreadContextClassLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

@Name("Spark_Structured_StreamETL")
@Description("spark2.x Structured streaming StreamETL")
public class Stream2EtlActuator
        implements JobActuator
{
    @Inject private YarnClient yarnClient;
    @Inject private SparkAppLauncher appLauncher;

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow flow)
    {
        return new SparkJobHandle<>(buildJob(jobId, flow, (URLClassLoader) this.getClass().getClassLoader()));
    }

    @Override
    public JobContainer createJobContainer(@NotNull Job job, Optional<String> jobInfo)
    {
        final JobContainer yarnJobContainer = new YarnJobContainer(yarnClient, jobInfo)
        {
            @Override
            public Optional<String> run()
                    throws Exception
            {
                ApplicationId yarnAppId = appLauncher.run(job);
                this.setYarnAppId(yarnAppId);
                TimeUnit.SECONDS.sleep(30);
                Socket sock = new Socket();
                sock.connect(new InetSocketAddress(InetAddress.getLocalHost(), 7102), 2_000);
                try (OutputStream out = sock.getOutputStream()) {
                    out.write(Serializables.serialize((SparkJobHandle) job.getJobHandle()));
                }
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

    private static Supplier<App<SparkSession, Dataset<Row>>> buildJob(String jobId, Flow flow, URLClassLoader classLoader)
    {
        final AtomicBoolean isComplic = new AtomicBoolean(true);

        Supplier<App<SparkSession, Dataset<Row>>> appGetter = (Supplier<App<SparkSession, Dataset<Row>>> & Serializable) () -> new App<SparkSession, Dataset<Row>>()
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
                return new StructuredPluginLoader()
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
            public Graph<Dataset<Row>> build()
            {
                return App.super.build(jobId, flow);
            }
        };

        try {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setCallable(() -> {
                        App<SparkSession, Dataset<Row>> app = appGetter.get();
                        app.build().run();
                        return 1;
                    })
                    .addUserURLClassLoader(classLoader)
                    .build();
            launcher.startAndGet(classLoader);

            isComplic.set(false);
            return appGetter;
        }
        catch (IOException | ClassNotFoundException | JVMRunningException e) {
            throw new SylphException(JOB_BUILD_ERROR, "JOB_BUILD_ERROR", e);
        }
    }
}
