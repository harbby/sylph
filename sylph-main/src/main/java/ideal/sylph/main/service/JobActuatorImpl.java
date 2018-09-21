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
package ideal.sylph.main.service;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobActuatorHandle;

import java.net.URLClassLoader;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JobActuatorImpl
        implements JobActuator
{
    private final long startTime = System.currentTimeMillis();
    private final JobActuator.ActuatorInfo info;
    private final JobActuatorHandle jobActuatorHandle;

    private final String name;
    private final String description;

    JobActuatorImpl(JobActuatorHandle jobActuatorHandle)
    {
        this.jobActuatorHandle = requireNonNull(jobActuatorHandle, "jobActuatorHandle is null");
        this.name = buildName(jobActuatorHandle);
        this.description = buildDescription(jobActuatorHandle);
        JobActuator.Mode mode = jobActuatorHandle.getClass().getAnnotation(JobActuator.Mode.class);

        this.info = new JobActuator.ActuatorInfo()
        {
            @Override
            public String getName()
            {
                return name;
            }

            @Override
            public String getDescription()
            {
                return description;
            }

            @Override
            public long getCreateTime()
            {
                return startTime;
            }

            @Override
            public String getVersion()
            {
                return "none";
            }

            @Override
            public ModeType getMode()
            {
                return mode != null ? mode.value() : ModeType.OTHER;
            }
        };
    }

    @Override
    public JobActuatorHandle getHandle()
    {
        return jobActuatorHandle;
    }

    @Override
    public ActuatorInfo getInfo()
    {
        return info;
    }

    @Override
    public URLClassLoader getHandleClassLoader()
    {
        return (URLClassLoader) jobActuatorHandle.getClass().getClassLoader();
    }

    private String buildName(JobActuatorHandle jobActuator)
    {
        String errorMessage = jobActuator.getClass().getName() + " Missing @Name annotation";
        Name actuatorName = jobActuator.getClass().getAnnotation(Name.class);
        String name = requireNonNull(actuatorName, errorMessage).value();
        checkState(name.length() > 0, errorMessage);
        return name;
    }

    private String buildDescription(JobActuatorHandle jobActuator)
    {
        String errorMessage = jobActuator.getClass().getName() + " Missing @Name annotation";
        Description description = jobActuator.getClass().getAnnotation(Description.class);
        return requireNonNull(description, errorMessage).value();
    }
}
