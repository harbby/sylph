package ideal.sylph.spi.job;

import ideal.sylph.spi.exception.SylphException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static ideal.sylph.spi.exception.StandardErrorCode.CONNECTION_ERROR;
import static ideal.sylph.spi.exception.StandardErrorCode.UNKNOWN_ERROR;
import static ideal.sylph.spi.job.Job.Status.RUNNING;
import static ideal.sylph.spi.job.Job.Status.STARTING;
import static ideal.sylph.spi.job.Job.Status.START_ERROR;
import static ideal.sylph.spi.job.Job.Status.STOP;
import static java.util.Objects.requireNonNull;

/**
 * Container that runs the yarn job
 */
public abstract class YarnJobContainer
        implements JobContainer
{
    private static final Logger logger = LoggerFactory.getLogger(YarnJobContainer.class);
    private final String jobId;
    protected Job.Status status = STOP;
    private YarnClient yarnClient;

    public YarnJobContainer(String jobId, YarnClient yarnClient)
    {
        this.jobId = requireNonNull(jobId, "jobId is null");
        this.yarnClient = requireNonNull(yarnClient, "yarnClient is null");
    }

    @Override
    public String getRunId()
    {
        ApplicationId yarnAppId = getYarnAppId();
        return yarnAppId == null ? "null" : yarnAppId.toString();
    }

    public abstract ApplicationId getYarnAppId();

    @Override
    public void shutdown()
    {
        try {
            yarnClient.killApplication(getYarnAppId());
        }
        catch (YarnException | IOException e) {
            throw new SylphException(UNKNOWN_ERROR, "YarnJobContainer shutdown fail", e);
        }
    }

    @Override
    public Job.Status getStatus()
    {
        if (this.status == START_ERROR) {
            return this.status;
        }
        ApplicationId yarnAppId = getYarnAppId();
        YarnApplicationState yarnAppStatus = getYarnAppStatus(yarnAppId);
        if (YarnApplicationState.ACCEPTED.equals(yarnAppStatus) || YarnApplicationState.RUNNING.equals(yarnAppStatus)) {  //运行良好
            this.status = RUNNING;
        }
        else {
            if (this.status != STARTING) {
                this.status = STOP;
            }
        }
        return status;
    }

    @Override
    public String getJobUrl()
    {
        try {
            //String proxyUrl = yarnClient.getApplicationReport(getYarnAppId()).getTrackingUrl();
            String originalUrl = yarnClient.getApplicationReport(getYarnAppId()).getOriginalTrackingUrl();
            return originalUrl;
        }
        catch (YarnException | IOException e) {
            return "UNKNOWN";
        }
        //throw new UnsupportedOperationException("this method have't support!");
    }

    /**
     * 获取yarn Job运行情况
     */
    private YarnApplicationState getYarnAppStatus(ApplicationId applicationId)
    {
        try {
            ApplicationReport app = yarnClient.getApplicationReport(applicationId); //获取某个指定的任务
            return app.getYarnApplicationState();
        }
        catch (ApplicationNotFoundException e) {  //app 不存在与yarn上面
            return null;
        }
        catch (YarnException | IOException e) {
            throw new SylphException(CONNECTION_ERROR, e);
        }
    }
}
