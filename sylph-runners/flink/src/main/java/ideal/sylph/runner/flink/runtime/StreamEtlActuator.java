package ideal.sylph.runner.flink.runtime;

import com.google.inject.Inject;
import ideal.sylph.runner.flink.FlinkApp;
import ideal.sylph.runner.flink.FlinkJob;
import ideal.sylph.runner.flink.GraphApp;
import ideal.sylph.runner.flink.utils.FlinkJobUtil;
import ideal.sylph.runner.flink.yarn.JobLauncher;
import ideal.sylph.runner.flink.yarn.YarnClusterConfiguration;
import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.YarnJobContainer;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import static com.google.common.base.MoreObjects.toStringHelper;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static ideal.sylph.spi.job.Job.Status.STARTING;
import static ideal.sylph.spi.job.Job.Status.START_ERROR;

@Name("StreamETL")
@Description("this is stream etl Actuator")
public class StreamEtlActuator
        implements JobActuator
{
    private static final Logger logger = LoggerFactory.getLogger(StreamEtlActuator.class);
    @Inject
    private YarnClusterConfiguration configuration;
    @Inject
    private YarnClient yarnClient;

    @NotNull
    @Override
    public Job formJob(String jobId, Flow flow)
    {
        try {
            final FlinkApp app = new GraphApp(jobId, flow);
            return FlinkJobUtil.createJob("StreamETL", jobId, app, flow);
        }
        catch (Exception e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }
    }

    @Override
    public JobContainer execJob(Job job)
    {
        return new YarnJobContainer(job.getId(), yarnClient)
        {
            private ApplicationId yarnAppId = null;

            @Override
            public ApplicationId getYarnAppId()
            {
                return yarnAppId;
            }

            @Override
            public void run()
            {
                this.status = STARTING;
                //logger.warn("Job {}[{}]状态为:{} 即将进行重新启动", job.getId(), this.getYarnAppId(), this.getStatus());
                /*
                 * 通过这个 修改当前YarnClient的ClassLoader的为当前sdk的加载器
                 * 默认是jvm的AppLoader,会出现 akka.version not setting的错误 原因是找不到akka相关jar包
                 * */
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                JobLauncher jobLauncher = new JobLauncher(configuration, yarnClient);
                try {
                    this.yarnAppId = jobLauncher.createApplication();
                    logger.info("Instantiating flinkSqlJob {} at yarnId {}", job.getId(), yarnAppId);
                    jobLauncher.start((FlinkJob) job, yarnAppId);
                }
                catch (Exception e) {
                    this.status = START_ERROR;
                    logger.warn("job {} start error", job.getId(), e);
                }
            }
        };
    }

    @Override
    public String toString()
    {
        toStringHelper(this)
                .add("name", "streamSql")
                .add("description", ".....");
        throw new UnsupportedOperationException("this method have't support!");
    }
}
