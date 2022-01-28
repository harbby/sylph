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
package com.github.harbby.sylph.spi.job;

import com.github.harbby.sylph.spi.OperatorType;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

public abstract class JobParser
        implements Serializable
{
    public Set<DependOperator> getDependOperators()
    {
        return Collections.emptySet();
    }

    @Override
    public abstract String toString();

    public static class DependOperator
            implements Serializable
    {
        private final String connector;
        private final OperatorType type;
        private String className;

        public DependOperator(String connector, OperatorType type)
        {
            this.connector = connector;
            this.type = type;
        }

        public static DependOperator of(String className, OperatorType type)
        {
            return new DependOperator(className, type);
        }

        public void setClassName(String className)
        {
            this.className = className;
        }

        public String getClassName()
        {
            return className;
        }

        public OperatorType getType()
        {
            return type;
        }

        public String getConnector()
        {
            return connector;
        }
    }
}
