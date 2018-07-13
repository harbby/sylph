package ideal.sylph.main.service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.model.EdgeInfo;
import ideal.sylph.spi.model.NodeInfo;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * default flow model
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class YamlFlow
        implements Flow
{
    static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private final List<NodeInfo> nodes;
    private final List<EdgeInfo> edges;
    private final String jobId;
    private final String type;
    private final Map<String, ?> config;

    @JsonCreator
    public YamlFlow(
            @JsonProperty("nodes") List<NodeInfo> nodes,
            @JsonProperty("edges") List<EdgeInfo> edges,
            @JsonProperty("jobId") String jobId,
            @JsonProperty("job.config") Map<String, ?> config,
            @JsonProperty("type") String type
    )
    {
        this.nodes = requireNonNull(nodes, "nodes must not null");
        this.edges = requireNonNull(edges, "edges must not null");
        this.jobId = requireNonNull(jobId, "jobId must not null");
        this.config = requireNonNull(config, "job.config must not null");
        this.type = requireNonNull(type, "type must not null");
    }

    @Override
    @JsonProperty
    public String getJobId()
    {
        return jobId;
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
    @JsonProperty
    public String getType()
    {
        return type;
    }

    public Map<String, ?> getConfig()
    {
        return config;
    }

    @Override
    public String toYamlDag()
            throws JsonProcessingException
    {
        return MAPPER.writeValueAsString(this);
    }

    /**
     * 加载yaml
     */
    public static YamlFlow load(File file)
            throws IOException
    {
        return MAPPER.readValue(file, YamlFlow.class);
    }

    public static YamlFlow load(String dag)
            throws IOException
    {
        return MAPPER.readValue(dag, YamlFlow.class);
    }
}
