package ideal.sylph.api;

import java.util.Map;
import java.util.function.Function;

public interface NodeLoader<T, R>
{
    public Function<R, R> loadSource(final T sess, final Map<String, Object> pluginConfig);

    public Function<R, R> loadTransform(final Map<String, Object> pluginConfig);

    public Function<R, R> loadSink(final Map<String, Object> pluginConfig);
}
