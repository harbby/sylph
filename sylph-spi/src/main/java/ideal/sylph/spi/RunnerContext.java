package ideal.sylph.spi;

import ideal.sylph.spi.model.PipelinePluginManager;

import java.util.Set;

public interface RunnerContext
{
    public Set<PipelinePluginManager.PipelinePluginInfo> getFindPlugins();
}
