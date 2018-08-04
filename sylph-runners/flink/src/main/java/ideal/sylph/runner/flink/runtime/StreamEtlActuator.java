package ideal.sylph.runner.flink.runtime;

import com.google.inject.Inject;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.common.proxy.DynamicProxy;
import ideal.sylph.runner.flink.FlinkJobHandle;
import ideal.sylph.runner.flink.utils.FlinkJobUtil;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.spi.classloader.ThreadContextClassLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

@Name("StreamETL")
@Description("this is stream etl Actuator")
public class StreamEtlActuator
        implements JobActuatorHandle
{
    private static final Logger logger = LoggerFactory.getLogger(StreamEtlActuator.class);
    @Inject private FlinkYarnJobLauncher jobLauncher;

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow flow, URLClassLoader jobClassLoader)
    {
        try {
            return FlinkJobUtil.createJob(jobId, flow, jobClassLoader);
        }
        catch (Exception e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }
    }

    @Override
    public JobContainer createJobContainer(@NotNull Job job, Optional<String> jobInfo)
    {
        JobContainer yarnJobContainer = new YarnJobContainer(jobLauncher.getYarnClient(), jobInfo)
        {
            @Override
            public Optional<String> run()
                    throws Exception
            {
                ApplicationId yarnAppId = jobLauncher.createApplication();
                this.setYarnAppId(yarnAppId);
                logger.info("Instantiating flinkSqlJob {} at yarnId {}", job.getId(), yarnAppId);
                jobLauncher.start((FlinkJobHandle) job.getJobHandle(), yarnAppId);
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "streamSql")
                .add("description", ".....")
                .toString();
    }
}
