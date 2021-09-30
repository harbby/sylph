/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.sylph.runner.flink;

import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.sylph.api.RealTimeSink;
import com.github.harbby.sylph.api.Sink;
import com.github.harbby.sylph.api.Source;

import java.util.Map;
import java.util.function.Function;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.sylph.spi.utils.PluginFactory.getPluginInstance;

public class FlinkOperatorFactory<D>
{
    private final IocFactory iocFactory;
    private final Function<RealTimeSink, Sink<D>> sinkCastFunc;

    private FlinkOperatorFactory(IocFactory iocFactory, Function<RealTimeSink, Sink<D>> sinkCastFunc)
    {
        this.iocFactory = iocFactory;
        this.sinkCastFunc = sinkCastFunc;
    }

    public static <D> FlinkOperatorFactory<D> createFactory(IocFactory iocFactory, Function<RealTimeSink, Sink<D>> sinkCastFunc)
    {
        return new FlinkOperatorFactory<>(iocFactory, sinkCastFunc);
    }

    public Sink<D> newInstanceSink(
            Class<?> operatorClass,
            final Map<String, Object> config)
    {
        Object t = getPluginInstance(iocFactory, operatorClass, config);
        if (t instanceof RealTimeSink) {
            return sinkCastFunc.apply((RealTimeSink) t);
        }
        else if (t instanceof Sink) {
            @SuppressWarnings("unchecked")
            Sink<D> sink = (Sink<D>) t;
            return sink;
        }
        else {
            throw new IllegalStateException();
        }
    }

    public Source<D> newInstanceSource(
            Class<?> operatorClass,
            final Map<String, Object> config)
    {
        checkState(Source.class.isAssignableFrom(operatorClass));
        return getPluginInstance(iocFactory, operatorClass.asSubclass(Source.class), config);
    }
}
