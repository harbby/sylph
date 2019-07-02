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

import java.io.File;
import java.net.URLClassLoader;

public interface JobEngine
{
    ContainerFactory getFactory();

    URLClassLoader getHandleClassLoader();

    String getName();

    String getDescription();

    long getCreateTime();

    Job compileJob(JobStore.DbJob dbJob, File jobWorkDir)
            throws Exception;

    default String getVersion()
    {
        return "1.0";
    }
}
