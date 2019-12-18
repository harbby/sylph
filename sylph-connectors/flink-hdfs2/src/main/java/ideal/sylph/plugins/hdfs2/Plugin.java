package ideal.sylph.plugins.hdfs2;

import com.github.harbby.gadtry.collection.mutable.MutableSet;
import ideal.sylph.etl.Operator;

import java.util.Set;

public class Plugin
        implements ideal.sylph.etl.Plugin
{
    @Override
    public Set<Class<? extends Operator>> getConnectors()
    {
        return MutableSet.<Class<? extends Operator>>builder()
                .add(HdfsSink2.class)
                .build();
    }
}
