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
package ideal.sylph.spi.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EdgeInfo
        implements Serializable
{
    private final String labelText;

    private final String inNodeId;
    private final String outNodeId;

    @JsonCreator
    public EdgeInfo(
            @JsonProperty("labelText") String labelText,
            @JsonProperty("uuids") String[] uuids
    )
    {
        checkArgument(uuids != null && uuids.length == 2, "uuids is null or not is String[2]");
        this.labelText = labelText;
        this.inNodeId = uuids[0];
        this.outNodeId = uuids[1];
    }

    @JsonProperty
    public String getLabelText()
    {
        return labelText;
    }

    @JsonProperty("uuids")
    public String[] getUuids()
    {
        return new String[] {inNodeId, outNodeId};
    }

    @JsonIgnore
    public String getInNodeId()
    {
        return inNodeId;
    }

    @JsonIgnore
    public String getOutNodeId()
    {
        return outNodeId;
    }
}
