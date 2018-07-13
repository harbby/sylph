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
    private final String nodeData;
    private final int nodeX;
    private final int nodeY;

    @JsonCreator
    public NodeInfo(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("nodeText") String nodeType,
            @JsonProperty("nodeConfig") Map<String, Object> nodeConfig,
            @JsonProperty("nodeData") String nodeData,
            @JsonProperty("nodeX") int nodeX,
            @JsonProperty("nodeY") int nodeY
    )
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.nodeType = nodeType;
        this.nodeData = nodeData;
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

    @JsonProperty
    public String getNodeData()
    {
        return nodeData;
    }

    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    @JsonProperty("nodeText")
    public String getNodeType()
    {
        return nodeType;
    }
}
