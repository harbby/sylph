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
package ideal.sylph.main.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class ServerMainConfig
{
    private String metadataPath;
    private String jobWorkDir;
    private String runMode = "yarn";

    @Config("server.metadata.path")
    @ConfigDescription("server.metadata.path location")
    public ServerMainConfig setMetadataPath(String metadataPath)
    {
        this.metadataPath = metadataPath;
        return this;
    }

    @NotNull(message = "server.metadata.path not setting")
    public String getMetadataPath()
    {
        return metadataPath;
    }

    @Config("server.jobstore.workpath")
    @ConfigDescription("server.jobstore.workpath is job local working dir")
    public void setJobWorkDir(String jobWorkDir)
    {
        this.jobWorkDir = jobWorkDir;
    }

    @NotNull(message = "server.jobstore.workpath not setting")
    public String getJobWorkDir()
    {
        return jobWorkDir;
    }

    @Config("job.runtime.mode")
    @ConfigDescription("job.runtime.mode, yarn or local")
    public void setRunMode(String runMode)
    {
        this.runMode = runMode;
    }

    @NotNull(message = "job.runtime.mode not setting")
    public String getRunMode()
    {
        return runMode;
    }
}
