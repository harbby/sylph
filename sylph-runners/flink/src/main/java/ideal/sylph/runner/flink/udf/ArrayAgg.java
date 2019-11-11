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
package ideal.sylph.runner.flink.udf;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

/**
 * udaf  presto array_agg
 */
public final class ArrayAgg
        extends AggregateFunction<Object[], Set<Object>>
{
    @Override
    public Set<Object> createAccumulator()
    {
        return new HashSet<>();
    }

    @Override
    public Object[] getValue(Set<Object> acc)
    {
        return acc.toArray();
    }

    public void accumulate(Set<Object> acc, Object iValue)
    {
        acc.add(iValue);
    }

    /**
     * go back
     */
    public void retract(Set<Object> acc, Object iValue)
    {
        acc.remove(iValue);
    }

    /**
     * merge partition
     */
    public void merge(Set<Object> acc, Iterable<Set<Object>> accs)
    {
        for (Set<Object> it : accs) {
            acc.addAll(it);
        }
    }

    /**
     * init
     */
    public void resetAccumulator(Set<Object> acc)
    {
        acc.clear();
    }
}
