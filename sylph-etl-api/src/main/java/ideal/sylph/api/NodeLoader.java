package ideal.sylph.api;

import java.util.Map;
import java.util.function.UnaryOperator;

public interface NodeLoader<T, R>
{
    public UnaryOperator<R> loadSource(final T sess, final Map<String, Object> pluginConfig);

    public UnaryOperator<R> loadTransform(final Map<String, Object> pluginConfig);

    public UnaryOperator<R> loadSink(final Map<String, Object> pluginConfig);
}
