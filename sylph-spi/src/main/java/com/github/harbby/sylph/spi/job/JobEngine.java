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
package com.github.harbby.sylph.spi.job;

import com.github.harbby.gadtry.jvm.JVMException;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface JobEngine
        extends Serializable
{
    /**
     * building job
     *
     * @param jobParser input Flow
     * @return JobHandel
     * @throws JVMException Throw it if the child process fails to compile
     */
    public Serializable compileJob(
            JobParser jobParser,
            JobConfig jobConfig)
            throws Exception;

    JobParser analyze(String jobDsl)
            throws IOException;

    List<Class<?>> keywords();
}
