package ideal.sylph.spi.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.spi.exception.SylphException;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_CONFIG_ERROR;
import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SqlFile
        implements Serializable
{
    private static final transient ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private final Map<String, Object> source;
    private final String transform;
    private final String sink;
    private final String type;

    @JsonCreator
    public SqlFile(
            @JsonProperty("source") Map<String, Object> source,
            @JsonProperty("transform") String transform,
            @JsonProperty("sink") String sink,
            @JsonProperty("type") String type
    )
    {
        this.source = requireNonNull(source, "source must not null");
        this.transform = requireNonNull(transform, "transform must not null");
        this.sink = requireNonNull(sink, "sink must not null");
        this.type = requireNonNull(type, "type must not null");
    }

    @JsonProperty
    public Map<String, Object> getSource()
    {
        return source;
    }

    @JsonProperty
    public String getTransform()
    {
        return transform;
    }

    @JsonProperty
    public String getSink()
    {
        return sink;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    /**
     * 获取任务的描述图 yaml格式的
     */
    public String getDagWithYaml()
    {
        try {
            return MAPPER.writeValueAsString(getDagWithMap());
        }
        catch (JsonProcessingException e) {
            throw new SylphException(JOB_CONFIG_ERROR, "序列化 dag文件失败", e);
        }
    }

    /**
     * json 格式的
     */
    public String getDagWithJson()
            throws JsonProcessingException
    {
        return new ObjectMapper().writeValueAsString(getDagWithMap());
    }

    public Map<String, Object> getDagWithMap()
    {
        return ImmutableMap.of(
                "type", type,
                "source", source,
                "transform", transform,
                "sink", sink
        );
    }

    /**
     * 加载yaml
     */
    public static SqlFile load(File file)
            throws IOException
    {
        return MAPPER.readValue(file, SqlFile.class);
    }

    public static SqlFile load(String dag)
            throws IOException
    {
        return MAPPER.readValue(dag, SqlFile.class);
    }
}
