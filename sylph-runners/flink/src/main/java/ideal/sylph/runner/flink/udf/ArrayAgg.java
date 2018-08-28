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
        extends AggregateFunction<Object[], ArrayAgg.WeightedAvgAccum>
{
    @Override
    public WeightedAvgAccum createAccumulator()
    {
        return new WeightedAvgAccum();
    }

    @Override
    public Object[] getValue(WeightedAvgAccum acc)
    {
        return acc.set.toArray();
    }

    public void accumulate(WeightedAvgAccum acc, Object iValue)
    {
        acc.set.add(iValue);
    }

    /**
     * go back
     */
    public void retract(WeightedAvgAccum acc, Object iValue)
    {
        acc.set.remove(iValue);
    }

    /**
     * merge partition
     */
    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it)
    {
        for (WeightedAvgAccum avgAccum : it) {
            acc.set.addAll(avgAccum.set);
        }
    }

    /**
     * init
     */
    public void resetAccumulator(WeightedAvgAccum acc)
    {
        acc.set.clear();
    }

    /**
     * status
     */
    public static class WeightedAvgAccum
    {
        private final Set<Object> set = new HashSet<>();
    }
}
