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
package ideal.sylph.runner.flink;

import io.airlift.configuration.Config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;

public class FlinkRunnerConfig
{
    private int serverPort = 8080;
    final File flinkJarFile = getFlinkJarFile();

    @Config("server.http.port")
    public FlinkRunnerConfig setServerPort(int serverPort)
    {
        this.serverPort = serverPort;
        return this;
    }

    @Min(1000)
    public int getServerPort()
    {
        return serverPort;
    }

    @NotNull
    public File getFlinkJarFile()
    {
        return flinkJarFile;
    }
}
