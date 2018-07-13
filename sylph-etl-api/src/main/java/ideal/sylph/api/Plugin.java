package ideal.sylph.api;

import ideal.sylph.api.etl.RealTimeSink;
import ideal.sylph.api.etl.RealTimeTransForm;
import ideal.sylph.api.etl.Sink;
import ideal.sylph.api.etl.Source;
import ideal.sylph.api.etl.TransForm;

import java.util.Collections;
import java.util.Set;

/**
 * 用户开发插件包
 */
public interface Plugin
{
    default Set<Class<? extends Source>> getSource()
    {
        return Collections.emptySet();
    }

    default Set<Class<? extends TransForm>> getTransForm()
    {
        return Collections.emptySet();
    }

    default Set<Class<? extends RealTimeTransForm>> getRealTimeTransForm()
    {
        return Collections.emptySet();
    }

    default Set<Class<? extends Sink>> getSink()
    {
        return Collections.emptySet();
    }

    default Set<Class<? extends RealTimeSink>> getRealTimeSink()
    {
        return Collections.emptySet();
    }
}
