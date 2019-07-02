package ideal.sylph.cli;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class QueryResult
{
    private List<String> columns;
    private Iterable<List<Object>> data;

    @JsonCreator
    public QueryResult(
            @JsonProperty("columns") List<String> columns,
            @JsonProperty("data") Iterable<List<Object>> data)
    {
        this.columns = columns;
        this.data = data;
    }

    @JsonProperty
    public List<String> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Iterable<List<Object>> getData()
    {
        return data;
    }
}
