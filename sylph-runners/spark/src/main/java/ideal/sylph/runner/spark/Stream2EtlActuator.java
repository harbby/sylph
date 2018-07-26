package ideal.sylph.runner.spark;

import com.google.inject.Inject;
import ideal.sylph.common.base.Serializables;
import ideal.sylph.common.jvm.JVMLauncher;
import ideal.sylph.common.jvm.JVMLaunchers;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

@Name("Spark_Structured_StreamETL")
@Description("spark2.x Structured streaming StreamETL")
public class Stream2EtlActuator
        implements JobActuator
{
    @Inject private YarnClient yarnClient;
    @Inject private SparkAppLauncher appLauncher;

    @Override
    public Job formJob(String jobId, Flow flow)
    {
        buildJob(jobId, flow);
        return new SparkJob()
        {
            @Override
            public String getId()
            {
                return jobId;
            }

            @Override
            public String getActuatorName()
            {
                return "Spark_Structured_StreamETL";
            }

            @Override
            public Flow getFlow()
            {
                return flow;
            }
        };
    }

    @Override
    public JobContainer createJobContainer(Job job, Optional<String> jobInfo)
    {
        JobContainer yarnJobContainer = new YarnJobContainer(yarnClient, jobInfo)
        {
            @Override
            public Optional<String> run()
                    throws Exception
            {
                ApplicationId yarnAppId = appLauncher.run((SparkJob) job);
                this.setYarnAppId(yarnAppId);
                TimeUnit.SECONDS.sleep(30);
                Socket sock = new Socket();
                sock.connect(new InetSocketAddress(InetAddress.getLocalHost(), 7102), 2_000);
                try (OutputStream out = sock.getOutputStream()) {
                    out.write(Serializables.serialize(job.getFlow()));
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

    private static void buildJob(String jobId, Flow flow)
    {
        try {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setCallable(() -> {
                        final SparkSession sparkSession = SparkSession.builder()
                                .appName("streamLoadTest")
                                .master("local[*]")
                                .getOrCreate();
                        App<SparkSession, Dataset<Row>> app = new App<SparkSession, Dataset<Row>>()
                        {
                            @Override
                            public NodeLoader<SparkSession, Dataset<Row>> getNodeLoader()
                            {
                                return new StructuredPluginLoader();
                            }

                            @Override
                            public SparkSession getContext()
                            {
                                return sparkSession;
                            }
                        };
                        app.build(jobId, flow).run();
                        return 1;
                    }).build();
            launcher.startAndGet(Stream2EtlActuator.class.getClassLoader());
        }
        catch (IOException | ClassNotFoundException e) {
            throw new SylphException(JOB_BUILD_ERROR, "JOB_BUILD_ERROR", e);
        }
    }
}
