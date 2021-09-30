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
package com.github.harbby.sylph.spi;

import com.github.harbby.sylph.api.Operator;
import com.github.harbby.sylph.api.Sink;
import com.github.harbby.sylph.api.Source;

public enum OperatorType
{
    source(Source.class),
    sink(Sink.class);

    private final Class<? extends Operator> value;

    OperatorType(Class<? extends Operator> value)
    {
        this.value = value;
    }

    public Class<? extends Operator> getValue()
    {
        return value;
    }
}
