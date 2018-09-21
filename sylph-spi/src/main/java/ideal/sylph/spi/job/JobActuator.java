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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URLClassLoader;

public interface JobActuator
{
    JobActuatorHandle getHandle();

    ActuatorInfo getInfo();

    URLClassLoader getHandleClassLoader();

    interface ActuatorInfo
    {
        String getName();

        String getDescription();

        long getCreateTime();

        String getVersion();

        ModeType getMode();
    }

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface Mode
    {
        ModeType value();
    }

    public static enum ModeType
    {
        STREAM_ETL(1),
        OTHER(2);

        private final int code;

        ModeType(int code)
        {
            this.code = code;
        }
    }
}
