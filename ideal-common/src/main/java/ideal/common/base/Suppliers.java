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

import java.io.Serializable;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class Suppliers
{
    private Suppliers() {}

    public static <T> Supplier<T> memoize(Supplier<T> delegate)
    {
        return delegate instanceof Suppliers.MemoizingSupplier ?
                delegate :
                new Suppliers.MemoizingSupplier<>(requireNonNull(delegate));
    }

    public static <T> Supplier<T> goLazy(Supplier<T> delegate)
    {
        return delegate instanceof Suppliers.MemoizingSupplier ?
                delegate :
                new Suppliers.MemoizingSupplier<>(requireNonNull(delegate));
    }

    static class MemoizingSupplier<T>
            implements Supplier<T>, Serializable
    {
        private final Supplier<T> delegate;
        private transient volatile boolean initialized = false;
        private transient T value;
        private static final long serialVersionUID = 0L;

        MemoizingSupplier(Supplier<T> delegate)
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
            return "Suppliers.memoize(" + this.delegate + ")";
        }
    }
}
