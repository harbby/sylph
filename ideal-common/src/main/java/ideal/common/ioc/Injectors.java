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
package ideal.common.ioc;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

import static com.google.common.base.Preconditions.checkState;

public class Injectors
{
    public static final Injectors INSTANCE = new Injectors();
    private static Logger logger = LoggerFactory.getLogger(Injectors.class);

    private Injectors() {}

    public final <T, O> T getInstance(Class<T> driver, Binds binds, Function<Class<?>, ?> other)
            throws InjectorException
    {
        try {
            return instance(driver, binds, other);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InjectorException(e);
        }
    }

    private static <T, O> T instance(Class<T> driver, Binds binds, Function<Class<?>, ?> other)
            throws Exception
    {
        @SuppressWarnings("unchecked")
        Constructor<T>[] constructors = (Constructor<T>[]) driver.getConstructors(); //public
        checkState(constructors.length == 1, String.format("%s has multiple public constructors, please ensure that there is only one", driver));
        final Constructor<T> constructor = constructors[0];

        if (constructor.getParameters().length == 0) {
            logger.info("plugin class [{}] not find 'no parameter' Constructor, using class.newInstance()", driver);
            return driver.newInstance();
        }
        constructor.setAccessible(true);

        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        for (Class<?> argType : constructor.getParameterTypes()) {
            Object value = binds.get(argType);
            if (value == null) {
                Object otherValue = other.apply((Class<O>) argType);
                checkState(otherValue != null, String.format("Cannot find instance of parameter [%s], unable to inject", argType));
                checkState(argType.isInstance(otherValue));
                builder.add(otherValue);
            }
            else {
                builder.add(value);
            }
        }
        return constructor.newInstance(builder.build().toArray());
    }

    public final <T, O> T getInstance(Class<T> driver, Binds binds)
            throws InjectorException
    {
        return getInstance(driver, binds, (type) -> null);
    }

    @FunctionalInterface
    public static interface Function<F0, F1>
    {
        F1 apply(F0 f0)
                throws Exception;
    }
}
