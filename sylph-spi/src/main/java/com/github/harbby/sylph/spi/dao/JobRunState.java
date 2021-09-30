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
package com.github.harbby.sylph.spi.dao;

public class JobRunState
{
    public enum Status
    {
        STOP,
        RUNNING,
        DEPLOYING,
        RUNNING_FAILED,
        DEPLOY_FAILED
    }

    private int jobId;
    private String runtimeType;
    private String runId;
    private long modifyTime;
    private Status status;
    private String webUi;
    private String type;  //engineName

    public int getJobId()
    {
        return jobId;
    }

    public void setJobId(int jobId)
    {
        this.jobId = jobId;
    }

    public String getRuntimeType()
    {
        return runtimeType;
    }

    public void setRuntimeType(String runtimeType)
    {
        this.runtimeType = runtimeType;
    }

    public String getRunId()
    {
        return runId;
    }

    public void setRunId(String runId)
    {
        this.runId = runId;
    }

    public long getModifyTime()
    {
        return modifyTime;
    }

    public void setModifyTime(long modifyTime)
    {
        this.modifyTime = modifyTime;
    }

    public void setStatus(Status status)
    {
        this.status = status;
    }

    public Status getStatus()
    {
        return status;
    }

    public void setWebUi(String webUi)
    {
        this.webUi = webUi;
    }

    public String getWebUi()
    {
        return webUi;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }
}
