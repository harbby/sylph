package ideal.sylph.runner.flink.yarn;

import com.google.inject.Inject;
import ideal.sylph.runner.flink.FlinkJob;
import ideal.sylph.runner.flink.JobParameter;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.clusterframework.messages.ShutdownClusterAfterJob;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * 负责和yarn打交道 负责job的管理 提交job 杀掉job 获取job 列表
 */
public class FlinkYarnJobLauncher
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkYarnJobLauncher.class);
    private static final FiniteDuration AKKA_TIMEOUT = new FiniteDuration(1, TimeUnit.MINUTES);

    @Inject
    private YarnClusterConfiguration clusterConf;
    @Inject
    private YarnClient yarnClient;

    public ApplicationId createApplication()
            throws IOException, YarnException
    {
        YarnClientApplication app = yarnClient.createApplication();
        return app.getApplicationSubmissionContext().getApplicationId();
    }

    public YarnClient getYarnClient()
    {
        return yarnClient;
    }

    public ApplicationReport getApplicationReport(ApplicationId yarnAppId)
            throws IOException, YarnException
    {
        return yarnClient.getApplicationReport(yarnAppId);
    }

    public void killApplication(ApplicationId yarnAppId)
            throws IOException, YarnException
    {
        yarnClient.killApplication(yarnAppId);
    }

    public void start(FlinkJob flinkJob, ApplicationId yarnAppId)
            throws Exception
    {
        final JobParameter jobState = flinkJob.getJobParameter();
        final YarnClusterDescriptor descriptor = new YarnClusterDescriptor(clusterConf, yarnClient, jobState, yarnAppId);

        start(descriptor, flinkJob.getJobGraph());
    }

    @VisibleForTesting
    void start(YarnClusterDescriptor descriptor, JobGraph job)
            throws Exception
    {
        YarnClusterClient client = descriptor.deploy();  //这一步提交到yarn
        try {
            client.runDetached(job, null);  //这一步提交到yarn 运行分离
            stopAfterJob(client, job.getJobID());
        }
        finally {
            client.shutdown();
            //清除临时目录
            try {
                FileSystem hdfs = FileSystem.get(clusterConf.conf());
                Path appDir = new Path(clusterConf.appRootDir(), client.getApplicationId().toString());
                hdfs.delete(appDir, true);
            }
            catch (IOException e) {
                logger.error("清除临时目录失败", e);
            }
        }
    }

    /**
     * 停止后的工作
     */
    private void stopAfterJob(ClusterClient client, JobID jobID)
    {
        requireNonNull(jobID, "The flinkLoadJob id must not be null");
        try {
            Future<Object> replyFuture =
                    client.getJobManagerGateway().ask(
                            new ShutdownClusterAfterJob(jobID),
                            AKKA_TIMEOUT);
            Await.ready(replyFuture, AKKA_TIMEOUT);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to tell application master to stop"
                    + " once the specified flinkLoadJob has been finished", e);
        }
    }
}
