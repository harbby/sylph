package org.ideal.sylph.api.pipeline;

import org.ideal.sylph.api.PipelinePlugin;

public interface Sink<T>
        extends PipelinePlugin
{
    void run(final T stream);
}
