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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import ideal.sylph.runner.flink.actuator.JobParameter;
import ideal.sylph.spi.job.JobConfig;

import java.io.File;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkJobConfig
        extends JobConfig
{
    private final JobParameter config;

    @JsonCreator
    public FlinkJobConfig(
            @JsonProperty("type") String type,
            @JsonProperty("config") JobParameter jobConfig
    )
    {
        super(type);
        this.config = requireNonNull(jobConfig, "jobConfig is null");
    }

    @JsonProperty(value = "config")
    public JobParameter getConfig()
    {
        return config;
    }

    /**
     * parser yaml config
     */
    public static FlinkJobConfig load(File file)
            throws IOException
    {
        return MAPPER.readValue(file, FlinkJobConfig.class);
    }

    public static FlinkJobConfig load(String string)
            throws IOException
    {
        return MAPPER.readValue(string, FlinkJobConfig.class);
    }

    public static FlinkJobConfig load(byte[] bytes)
            throws IOException
    {
        return MAPPER.readValue(bytes, FlinkJobConfig.class);
    }
}
