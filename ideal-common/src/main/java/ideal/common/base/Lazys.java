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
package ideal.common.base;

import ideal.common.function.Creater;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

public class Lazys
{
    private Lazys() {}

    public static <T> Creater<T> memoize(Creater<T> delegate)
    {
        return delegate instanceof LazySupplier ?
                delegate :
                new LazySupplier<>(requireNonNull(delegate));
    }

    public static <T> Creater<T> goLazy(Creater<T> delegate)
    {
        return memoize(delegate);
    }

    public static class LazySupplier<T>
            implements Serializable, Creater<T>
    {
        private final Creater<T> delegate;
        private transient volatile boolean initialized = false;
        private transient T value;
        private static final long serialVersionUID = 0L;

        LazySupplier(Creater<T> delegate)
        {
            this.delegate = delegate;
        }

        public T get()
        {
            if (!this.initialized) {
                synchronized (this) {
                    if (!this.initialized) {
                        T t = this.delegate.get();
                        this.value = t;
                        this.initialized = true;
                        return t;
                    }
                }
            }

            return this.value;
        }

        public String toString()
        {
            return "Lazys.memoize(" + this.delegate + ")";
        }
    }
}
