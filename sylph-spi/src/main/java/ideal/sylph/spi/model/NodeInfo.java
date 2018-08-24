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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeInfo
        implements Serializable
{
    private final String nodeId;
    private final String nodeType;
    private final Map<String, Object> nodeConfig;
    private final String nodeText;
    private final int nodeX;
    private final int nodeY;

    @JsonCreator
    public NodeInfo(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("nodeType") String nodeType,
            @JsonProperty("nodeConfig") Map<String, Object> nodeConfig,
            @JsonProperty("nodeText") String nodeText,
            @JsonProperty("nodeX") int nodeX,
            @JsonProperty("nodeY") int nodeY
    )
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.nodeType = nodeType;
        this.nodeText = nodeText;
        this.nodeConfig = nodeConfig;
        this.nodeX = nodeX;
        this.nodeY = nodeY;
    }

    @JsonProperty
    public int getNodeX()
    {
        return nodeX;
    }

    @JsonProperty
    public int getNodeY()
    {
        return nodeY;
    }

    @JsonProperty
    public Map<String, Object> getNodeConfig()
    {
        return nodeConfig;
    }

    @JsonProperty("nodeText")
    public String getNodeText()
    {
        return nodeText;
    }

    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    @JsonProperty("nodeType")
    public String getNodeType()
    {
        return nodeType;
    }
}
