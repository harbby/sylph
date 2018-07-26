package ideal.sylph.spi.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import ideal.sylph.spi.model.EdgeInfo;
import ideal.sylph.spi.model.NodeInfo;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * default flow model
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class YamlFlow
        implements Flow
{
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private final List<NodeInfo> nodes;
    private final List<EdgeInfo> edges;

    @JsonCreator
    public YamlFlow(
            @JsonProperty("nodes") List<NodeInfo> nodes,
            @JsonProperty("edges") List<EdgeInfo> edges
    )
    {
        this.nodes = requireNonNull(nodes, "nodes must not null");
        this.edges = requireNonNull(edges, "edges must not null");
    }

    @Override
    @JsonProperty
    public List<EdgeInfo> getEdges()
    {
        return edges;
    }

    @Override
    @JsonProperty
    public List<NodeInfo> getNodes()
    {
        return nodes;
    }

    @Override
    public String toString()
    {
        try {
            return MAPPER.writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 加载yaml
     */
    public static Flow load(File file)
            throws IOException
    {
        return MAPPER.readValue(file, YamlFlow.class);
    }

    public static Flow load(String dag)
            throws IOException
    {
        return MAPPER.readValue(dag, YamlFlow.class);
    }
}
