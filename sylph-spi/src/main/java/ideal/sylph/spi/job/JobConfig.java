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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import ideal.sylph.spi.exception.SylphException;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static ideal.sylph.spi.exception.StandardErrorCode.CONFIG_ERROR;
import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JobConfig
{
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private final String type;

    public JobConfig(@JsonProperty("type") String type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @NotNull
    @JsonProperty
    public final String getType()
    {
        return type;
    }

    @Override
    public final String toString()
    {
        try {
            return MAPPER.writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            throw new SylphException(CONFIG_ERROR, "Serialization configuration error", e);
        }
    }

    /**
     * parser yaml config
     */
    public static JobConfig load(File file)
            throws IOException
    {
        return MAPPER.readValue(file, JobConfig.class);
    }

    public static JobConfig load(String string)
            throws IOException
    {
        return MAPPER.readValue(string, JobConfig.class);
    }

    public static JobConfig load(byte[] bytes)
            throws IOException
    {
        return MAPPER.readValue(bytes, JobConfig.class);
    }

    public static JobConfig load(Map map)
    {
        return MAPPER.convertValue(map, JobConfig.class);
    }
}
