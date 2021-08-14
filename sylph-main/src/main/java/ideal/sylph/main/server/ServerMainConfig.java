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

import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.main.util.PropertiesUtil;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class ServerMainConfig
{
    private final String metadataPath;
    private final String jobWorkDir;
    private final String runMode;
    private final Map<String, String> config;

    @Autowired
    public ServerMainConfig(Properties properties)
    {
        Map<String, String> config = PropertiesUtil.fromProperties(properties);
        this.config = config;
        this.metadataPath = config.get("server.metadata.path");
        this.jobWorkDir = requireNonNull(config.get("server.jobstore.workpath"), "server.jobstore.workpath not setting");
        this.runMode = config.getOrDefault("job.runtime.mode", "yarn");
    }

    public String getMetadataPath()
    {
        return metadataPath;
    }

    @NotNull(message = "server.jobstore.workpath not setting")
    public String getJobWorkDir()
    {
        return jobWorkDir;
    }

    @NotNull(message = "job.runtime.mode not setting")
    public String getRunMode()
    {
        return runMode;
    }

    public File getPluginDir()
    {
        String dir = config.getOrDefault("plugin.operator.dir", "./plugins");
        return new File(dir);
    }
}
