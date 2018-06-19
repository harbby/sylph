package org.ideal.sylph.api;

import org.ideal.sylph.api.pipeline.RealTimeSink;
import org.ideal.sylph.api.pipeline.RealTimeTransForm;
import org.ideal.sylph.api.pipeline.Sink;
import org.ideal.sylph.api.pipeline.Source;
import org.ideal.sylph.api.pipeline.TransForm;

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
