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

import ideal.common.function.Creater;

public class IocFactoryImpl
        implements IocFactory
{
    private final Binds binds;

    IocFactoryImpl(Binds binds)
    {
        this.binds = binds;
    }

    /**
     * @throws InjectorException
     */
    public <T> T getInstance(Class<T> driver)
    {
        return getCreater(driver).get();
    }

    @Override
    public <T> Creater<T> getCreater(Class<T> driver)
    {
        return getCreater(driver, (driverClass) -> null);
    }

    @Override
    public <T> Binds getAllBeans()
    {
        return binds;
    }

    private <T> Creater<T> getCreater(Class<T> driver, IocFactory.Function<Class<?>, ?> other)
    {
        return () -> InternalContext.of(binds, other).get(driver);
    }

    /**
     * @throws InjectorException
     */
    public <T> T getInstance(Class<T> driver, IocFactory.Function<Class<?>, ?> other)
    {
        return getCreater(driver, other).get();
    }

    Binds getBinds()
    {
        return binds;
    }
}
