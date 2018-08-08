package ideal.sylph.spi;

import ideal.sylph.spi.model.PipelinePluginManager;

public interface RunnerContext
{
    public PipelinePluginManager getPluginManager();
}
