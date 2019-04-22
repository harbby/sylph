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
package ideal.sylph.runtime.yarn;

import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static ideal.sylph.spi.exception.StandardErrorCode.CONNECTION_ERROR;
import static ideal.sylph.spi.job.Job.Status.RUNNING;
import static ideal.sylph.spi.job.Job.Status.STOP;
import static java.util.Objects.requireNonNull;

public class YarnJobContainer
        implements JobContainer
{
    private static final Logger logger = LoggerFactory.getLogger(YarnJobContainer.class);
    private ApplicationId yarnAppId;
    private YarnClient yarnClient;
    private volatile Job.Status status = STOP;
    private volatile Future future;
    private volatile String webUi;

    private final Callable<Optional<ApplicationId>> runnable;

    public YarnJobContainer(YarnClient yarnClient, String jobInfo, Callable<Optional<ApplicationId>> runnable)
    {
        this.runnable = runnable;
        this.yarnClient = yarnClient;
        if (jobInfo != null) {
            this.yarnAppId = Apps.toAppID(jobInfo);
            this.setStatus(RUNNING);
            try {
                this.webUi = yarnClient.getApplicationReport(yarnAppId).getOriginalTrackingUrl();
            }
            catch (YarnException | IOException e) {
                throwsException(e);
            }
        }
    }

    @Override
    public synchronized void shutdown()
    {
        try {
            if (future != null && !future.isDone() && !future.isCancelled()) {
                future.cancel(true);
            }
        }
        finally {
            try {
                if (yarnAppId != null) {
                    yarnClient.killApplication(yarnAppId);
                }
            }
            catch (Exception e) {
                logger.error("kill yarn id {} failed", yarnAppId, e);
            }
        }
    }

    @Override
    public Optional<String> run()
            throws Exception
    {
        this.setYarnAppId(null);
        Optional<ApplicationId> applicationId = runnable.call();
        applicationId.ifPresent(this::setYarnAppId);
        this.webUi = yarnClient.getApplicationReport(yarnAppId).getOriginalTrackingUrl();
        return applicationId.map(ApplicationId::toString);
    }

    @Override
    public String getRunId()
    {
        return yarnAppId == null ? "none" : yarnAppId.toString();
    }

    public synchronized void setYarnAppId(ApplicationId appId)
    {
        this.yarnAppId = appId;
    }

    public ApplicationId getYarnAppId()
    {
        return yarnAppId;
    }

    @Override
    public String getJobUrl()
    {
        try {
            return "N/A".equals(webUi) ? yarnClient.getApplicationReport(yarnAppId).getOriginalTrackingUrl() : webUi;
        }
        catch (YarnException | IOException e) {
            throw throwsException(e);
        }
    }

    @Override
    public synchronized void setStatus(Job.Status status)
    {
        this.status = requireNonNull(status, "status is null");
    }

    @Override
    public synchronized Job.Status getStatus()
    {
        if (status == RUNNING) {
            return isRunning() ? RUNNING : STOP;
        }
        return status;
    }

    @Override
    public void setFuture(Future future)
    {
        this.future = future;
    }

    /**
     * 获取yarn Job运行情况
     */
    private boolean isRunning()
    {
        try {
            ApplicationReport app = yarnClient.getApplicationReport(getYarnAppId()); //获取某个指定的任务
            YarnApplicationState state = app.getYarnApplicationState();
            return YarnApplicationState.ACCEPTED.equals(state) || YarnApplicationState.RUNNING.equals(state);
        }
        catch (ApplicationNotFoundException e) {  //app 不存在与yarn上面
            return false;
        }
        catch (YarnException | IOException e) {
            throw new SylphException(CONNECTION_ERROR, e);
        }
    }
}
