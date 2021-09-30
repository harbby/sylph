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
package com.github.harbby.sylph.yarn;

import com.github.harbby.sylph.spi.job.DeployResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import static java.util.Objects.requireNonNull;

public class YarnDeployResponse
        implements DeployResponse
{
    private final ApplicationId yarnAppId;
    private final String webUi;

    public YarnDeployResponse(ApplicationId yarnAppId, String webUi)
    {
        this.yarnAppId = requireNonNull(yarnAppId);
        this.webUi = webUi;
    }

    @Override
    public String getRunId()
    {
        return yarnAppId.toString();
    }

    public ApplicationId getYarnAppId()
    {
        return yarnAppId;
    }

    @Override
    public String getAppWeb()
    {
        return webUi;
    }
}
