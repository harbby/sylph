package ideal.sylph;

import ideal.sylph.etl.Schema;

import java.io.Serializable;
import java.util.Map;

public interface TableContext
        extends Serializable
{
    public Schema getSchema();

    public String getTableName();

    public String getConnector();

    public Map<String, Object> withConfig();
}
