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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobInfo
{
    private final int jobId;
    private final String jobName;
    private final String queryText;
    private final String type;
    private final String config;
    private final String description;
    //---next state
    private JobRunState.Status status = JobRunState.Status.STOP;
    private String runId;
    private String appUrl;

    @JsonCreator
    public JobInfo(
            @JsonProperty("id") Integer jobId,
            @JsonProperty("jobName") String jobName,
            @JsonProperty("queryText") String queryText,
            @JsonProperty("type") String type,
            @JsonProperty("config") String config,
            @JsonProperty("description") String description)
    {
        this.jobId = requireNonNull(jobId, "jobId is null");
        this.jobName = requireNonNull(jobName, "jobName is null");
        this.queryText = requireNonNull(queryText, "queryText is null");
        this.type = requireNonNull(type, "type is null");
        this.config = requireNonNull(config, "config is null");
        this.description = description;
    }

    public void setStatus(JobRunState.Status status)
    {
        this.status = status;
    }

    public void setRunId(String runId)
    {
        this.runId = runId;
    }

    public void setAppUrl(String appUrl)
    {
        this.appUrl = appUrl;
    }

    @JsonProperty
    public int getJobId()
    {
        return jobId;
    }

    @JsonProperty
    public String getJobName()
    {
        return jobName;
    }

    @JsonProperty
    public String getQueryText()
    {
        return queryText;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public String getConfig()
    {
        return config;
    }

    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    @JsonProperty
    public JobRunState.Status getStatus()
    {
        return status;
    }

    @JsonProperty
    public String getRunId()
    {
        return runId;
    }

    @JsonProperty
    public String getAppUrl()
    {
        return appUrl;
    }
}
