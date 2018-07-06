package ideal.sylph.api;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface PluginLoader<T, R>
{
    public Function<R, Optional<R>> loadSource(final T sess, final Map<String, Object> pluginConfig);

    public Function<R, Optional<R>> loadTransform(final Map<String, Object> pluginConfig);

    public Function<R, Optional<R>> loadSink(final Map<String, Object> pluginConfig);
}
