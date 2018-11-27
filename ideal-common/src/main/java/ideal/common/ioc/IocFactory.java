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

import ideal.common.base.Lazys;
import ideal.common.function.Creater;

/**
 * harbby ioc
 */
public interface IocFactory
{
    /**
     * @throws InjectorException
     */
    public <T> T getInstance(Class<T> driver);

    /**
     * @throws InjectorException
     */
    public <T> T getInstance(Class<T> driver, IocFactory.Function<Class<?>, ?> other);

    public <T> Creater<T> getCreater(Class<T> driver);

    public <T> Binds getAllBeans();

    public static IocFactory create(Bean... beans)
    {
        final Binds.Builder builder = Binds.builder();
        final InternalContext context = InternalContext.of(builder.build(), (x) -> null);
        final Binder binder = new Binder()
        {
            @Override
            public <T> void bind(Class<T> key, T instance)
            {
                builder.bind(key, () -> instance);
            }

            @Override
            public <T> BinderBuilder<T> bind(Class<T> key)
            {
                return new BinderBuilder<T>()
                {
                    @Override
                    public void withSingle()
                    {
                        builder.bind(key, Lazys.goLazy(() -> context.getByNew(key)));
                    }

                    @Override
                    public BindingSetting by(Class<? extends T> createClass)
                    {
                        Creater<T> creater = () -> context.getByNew(createClass);
                        builder.bind(key, creater);
                        return () -> builder.bindUpdate(key, Lazys.goLazy(creater));
                    }

                    @Override
                    public void byInstance(T instance)
                    {
                        builder.bind(key, () -> instance);
                    }

                    @Override
                    public BindingSetting byCreater(Creater<? extends T> creater)
                    {
                        builder.bind(key, creater);
                        return () -> builder.bindUpdate(key, Lazys.goLazy(creater));
                    }

                    @Override
                    public BindingSetting byCreater(Class<? extends Creater<T>> createrClass)
                    {
                        try {
                            return this.byCreater(createrClass.newInstance());
                        }
                        catch (InstantiationException | IllegalAccessException e) {
                            throw new InjectorException(e);
                        }
                    }
                };
            }
        };

        for (Bean bean : beans) {
            bean.configure(binder);
        }
        Binds binds = builder.build();
        return new IocFactoryImpl(binds);
    }

    @FunctionalInterface
    public static interface Function<F0, F1>
    {
        F1 apply(F0 f0)
                throws Exception;
    }
}
