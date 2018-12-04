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
import com.github.harbby.gadtry.classloader.ThreadContextClassLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobContainerAbs;
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

import static ideal.sylph.spi.exception.StandardErrorCode.CONNECTION_ERROR;
import static ideal.sylph.spi.job.Job.Status.KILLING;
import static ideal.sylph.spi.job.Job.Status.RUNNING;

public abstract class YarnJobContainer
        extends JobContainerAbs
{
    private static final Logger logger = LoggerFactory.getLogger(YarnJobContainer.class);
    private ApplicationId yarnAppId;
    private YarnClient yarnClient;

    protected YarnJobContainer(YarnClient yarnClient, String jobInfo)
    {
        this.yarnClient = yarnClient;
        if (jobInfo != null) {
            this.yarnAppId = Apps.toAppID(jobInfo);
            this.setStatus(RUNNING);
        }
    }

    @Override
    public synchronized void shutdown()
    {
        try {
            this.setStatus(KILLING);
            if (yarnAppId != null) {
                yarnClient.killApplication(yarnAppId);
            }
        }
        catch (Exception e) {
            logger.error("kill yarn id {} failed", yarnAppId, e);
        }
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
    public boolean isRunning()
    {
        YarnApplicationState yarnAppStatus = getYarnAppStatus(yarnAppId);
        return YarnApplicationState.ACCEPTED.equals(yarnAppStatus) || YarnApplicationState.RUNNING.equals(yarnAppStatus);
    }

    @Override
    public String getJobUrl()
    {
        try {
            String originalUrl = yarnClient.getApplicationReport(yarnAppId).getOriginalTrackingUrl();
            return originalUrl;
        }
        catch (YarnException | IOException e) {
            throw new RuntimeException(e);
        }
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

    public static JobContainer proxy(JobContainer yarnJobContainer)
    {
        //----create JobContainer Proxy
        return AopFactory.proxy(JobContainer.class)
                .byInstance(yarnJobContainer)
                .around(proxyContext -> {
                    /*
                     * 通过这个 修改当前YarnClient的ClassLoader的为当前sdk的加载器
                     * 默认hadoop Configuration使用jvm的AppLoader,会出现 akka.version not setting的错误 原因是找不到akka相关jar包
                     * 原因是hadoop Configuration 初始化: this.classLoader = Thread.currentThread().getContextClassLoader();
                     * */
                    try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(yarnJobContainer.getClass().getClassLoader())) {
                        proxyContext.proceed();
                    }
                });
    }
}
