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
package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.net.URL;
import java.util.Collection;

public interface Job
{
    @NotNull
    public String getId();

    default String getDescription()
    {
        return "none";
    }

    File getWorkDir();

    Collection<URL> getDepends();

    ClassLoader getJobClassLoader();

    @NotNull
    String getActuatorName();

    @NotNull
    JobHandle getJobHandle();

    @NotNull
    JobConfig getConfig();

    @NotNull
    Flow getFlow();

    public enum Status
    {
        RUNNING(0),   //运行中
        STARTING(1),    // 启动中
        STOP(2),           // 停止运行
        STARTED_ERROR(3);          // 启动失败

        private final int status;

        Status(int code)
        {
            this.status = code;
        }
    }
}
