package ideal.sylph.plugins.hdfs2;

import com.github.harbby.gadtry.collection.mutable.MutableSet;
import ideal.sylph.etl.PipelinePlugin;

import java.util.Set;

public class Plugin
        implements ideal.sylph.etl.Plugin
{
    @Override
    public Set<Class<? extends PipelinePlugin>> getConnectors()
    {
        return MutableSet.<Class<? extends PipelinePlugin>>builder()
                .add(HdfsSink2.class)
                .build();
    }
}
