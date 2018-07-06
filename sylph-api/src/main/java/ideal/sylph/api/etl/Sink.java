package ideal.sylph.api.etl;

import ideal.sylph.api.PipelinePlugin;

public interface Sink<T>
        extends PipelinePlugin
{
    void run(final T stream);
}
