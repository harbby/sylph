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

import com.github.harbby.gadtry.aop.AopFactory;
import com.github.harbby.gadtry.base.Closeables;
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

    private YarnJobContainer(YarnClient yarnClient, Callable<Optional<ApplicationId>> runnable)
    {
        this.runnable = runnable;
        this.yarnClient = yarnClient;
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
            return "N/A".equals(webUi) ? yarnClient.getApplicationReport(yarnAppId).getTrackingUrl() : webUi;
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private YarnClient yarnClient;
        private Callable<Optional<ApplicationId>> submitter;
        private String lastRunId;
        private ClassLoader jobClassLoader;

        public Builder setYarnClient(YarnClient yarnClient)
        {
            this.yarnClient = yarnClient;
            return this;
        }

        public Builder setLastRunId(String lastRunId)
        {
            this.lastRunId = lastRunId;
            return this;
        }

        public Builder setSubmitter(Callable<Optional<ApplicationId>> submitter)
        {
            this.submitter = submitter;
            return this;
        }

        public Builder setJobClassLoader(ClassLoader jobClassLoader)
        {
            this.jobClassLoader = jobClassLoader;
            return this;
        }

        public JobContainer build()
        {
            final YarnJobContainer yarnJobContainer = new YarnJobContainer(yarnClient, submitter);
            if (lastRunId != null) {
                yarnJobContainer.yarnAppId = Apps.toAppID(lastRunId);
                yarnJobContainer.setStatus(RUNNING);
                try {
                    yarnJobContainer.webUi = yarnClient.getApplicationReport(yarnJobContainer.yarnAppId).getTrackingUrl();
                }
                catch (Exception e) {
                    logger.warn(e.getMessage());
                }
            }

            //----create JobContainer Proxy
            return AopFactory.proxy(JobContainer.class)
                    .byInstance(yarnJobContainer)
                    .around(proxyContext -> {
                        /*
                         * 通过这个 修改当前YarnClient的ClassLoader的为当前runner的加载器
                         * 默认hadoop Configuration使用jvm的AppLoader,会出现 akka.version not setting的错误 原因是找不到akka相关jar包
                         * 原因是hadoop Configuration 初始化: this.classLoader = Thread.currentThread().getContextClassLoader();
                         * */
                        try (Closeables ignored = Closeables.openThreadContextClassLoader(jobClassLoader)) {
                            return proxyContext.proceed();
                        }
                    });
        }
    }
}
