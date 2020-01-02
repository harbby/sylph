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
import com.fasterxml.jackson.databind.ObjectMapper;
import ideal.sylph.spi.utils.GenericTypeReference;
import ideal.sylph.spi.utils.JsonTextUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeInfo
        implements Serializable
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String nodeId;
    private final String nodeType;
    private final String nodeLable;
    private final Map<String, Object> nodeConfig;
    private final String nodeText;
    private final int nodeX;
    private final int nodeY;

    private final String driverClass;
    private final Map<String, Object> userConfig;

    @JsonCreator
    public NodeInfo(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("nodeLable") String nodeLable,
            @JsonProperty("nodeType") String nodeType,
            @Deprecated @JsonProperty("nodeConfig") Map<String, Object> nodeConfig,
            @JsonProperty("nodeText") String nodeText,
            @JsonProperty("nodeX") int nodeX,
            @JsonProperty("nodeY") int nodeY
    )
            throws IOException
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.nodeType = requireNonNull(nodeType, "nodeType is null");
        this.nodeLable = requireNonNull(nodeLable, "nodeLable is null");
        this.nodeText = nodeText;
        this.nodeConfig = nodeConfig;
        this.nodeX = nodeX;
        this.nodeY = nodeY;
        //--- parser driver class
        String json = JsonTextUtil.readJsonText(nodeText);
        Map<String, Map<String, Object>> nodeTextMap = MAPPER.readValue(json, new GenericTypeReference(Map.class, String.class, Map.class));
        this.driverClass = (String) requireNonNull(nodeTextMap.getOrDefault("plugin", Collections.emptyMap()).get("driver"), "config key driver is not setting");
        this.userConfig = requireNonNull(nodeTextMap.get("user"), "user config is null");
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

    @JsonProperty
    public String getNodeLable()
    {
        return nodeLable;
    }

    @JsonIgnore
    public String getDriverClass()
    {
        return driverClass;
    }

    @JsonIgnore
    public Map<String, Object> getUserConfig()
    {
        return userConfig;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeId", nodeId)
                .add("nodeLable", nodeLable)
                .add("nodeType", nodeType)
                .add("nodeConfig", nodeConfig)
                .add("nodeText", nodeText)
                .add("nodeX", nodeX)
                .add("nodeY", nodeY)
                .toString();
    }
}
