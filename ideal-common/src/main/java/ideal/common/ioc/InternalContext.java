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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

class InternalContext
{
    private final ThreadLocal<Set<Class<?>>> deps = ThreadLocal.withInitial(HashSet::new);
    private final IocFactory.Function<Class<?>, ?> other;
    private final Binds binds;

    private InternalContext(Binds binds, IocFactory.Function<Class<?>, ?> other)
    {
        this.binds = binds;
        this.other = other;
    }

    public static InternalContext of(Binds binds, IocFactory.Function<Class<?>, ?> other)
    {
        return new InternalContext(binds, other);
    }

    public <T> T get(Class<T> driver)
    {
        Set<Class<?>> depCLass = deps.get();
        depCLass.clear();
        depCLass.add(driver);

        T t = getInstance(driver);
        depCLass.clear();
        return t;
    }

    public <T> T getByNew(Class<T> driver)
    {
        Set<Class<?>> depCLass = deps.get();
        depCLass.clear();
        depCLass.add(driver);

        T t = getNewInstance(driver);
        depCLass.clear();
        return t;
    }

    private <T> T getInstance(Class<T> driver)
    {
        return binds.getOrDefault(driver, () -> getNewInstance(driver)).get();
    }

    private <T> T getNewInstance(Class<T> driver)
    {
        try {
            return newInstance(driver);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new InjectorException(e.getMessage(), e.getCause());
        }
        catch (Exception e) {
            throw new InjectorException(e);
        }
    }

    private boolean check(Class<?> type)
    {
        return !deps.get().contains(type);
    }

    private <T> T newInstance(Class<T> driver)
            throws Exception
    {
        final Constructor<T> constructor = selectConstructor(driver);
        constructor.setAccessible(true);

        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        for (Class<?> argType : constructor.getParameterTypes()) {
            checkState(argType != driver && check(argType), "Found a circular dependency involving " + driver + ", and circular dependencies are disabled.");

            Object otherValue = other.apply(argType);
            if (otherValue == null) {
                //Object value = binds.get(argType);
                Object value = getInstance(argType);
                checkState(value != null, String.format("Could not find a suitable constructor in [%s]. Classes must have either one (and only one) constructor annotated with @Autowired or a constructor that is not private(and only one).", argType));
                builder.add(value);
            }
            else {
                checkState(argType.isInstance(otherValue));
                builder.add(otherValue);
            }
        }

        T instance = constructor.newInstance(builder.build().toArray());
        return buildAnnotationFields(driver, instance);
    }

    private <T> T buildAnnotationFields(Class<T> driver, T instance)
            throws IllegalAccessException
    {
        for (Field field : driver.getDeclaredFields()) {
            Autowired autowired = field.getAnnotation(Autowired.class);
            if (autowired != null) {
                field.setAccessible(true);
                if (field.getType() == driver) {
                    field.set(instance, instance);
                }
                else {
                    field.set(instance, getInstance(field.getType()));
                }
            }
        }
        return instance;
    }

    private static <T> Constructor<T> selectConstructor(Class<T> driver)
    {
        @SuppressWarnings("unchecked")
        Constructor<T>[] constructors = (Constructor<T>[]) driver.getConstructors(); //public

        Constructor<T> noParameter = null;
        for (Constructor<T> constructor : constructors) {
            Autowired autowired = constructor.getAnnotation(Autowired.class);
            if (autowired != null) {
                return constructor;
            }
            if (constructor.getParameterCount() == 0) {
                //find 'no parameter' Constructor, using class.newInstance()";
                noParameter = constructor;
            }
        }

        if (noParameter != null) {
            return noParameter;
        }

        checkState(constructors.length == 1, String.format("%s has multiple public constructors, please ensure that there is only one", driver));
        return constructors[0];
    }
}
