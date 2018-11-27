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

public interface Binder
{
    public <T> void bind(Class<T> key, T instance);

    public <T> BinderBuilder<T> bind(Class<T> key);

    public interface BinderBuilder<T>
            extends BindingSetting
    {
        BindingSetting by(Class<? extends T> createClass);

        void byInstance(T instance);

        BindingSetting byCreater(Creater<? extends T> creater);

        BindingSetting byCreater(Class<? extends Creater<T>> createrClass);
    }

    public interface BindingSetting
    {
        public void withSingle();
    }
}
