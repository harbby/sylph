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
package com.github.harbby.sylph.runner.spark.kafka;

import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.InputDStream;
import scala.Option;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;

/**
 * @see org.apache.spark.streaming.dstream.MappedDStream
 */
public abstract class SylphKafkaOffset<T>
        extends DStream<T>
{
    private final DStream<T> parent;

    public SylphKafkaOffset(InputDStream<T> parent)
    {
        super(parent.ssc(), JavaSparkContext$.MODULE$.<T>fakeClassTag());
        this.parent = parent;
    }

    @Override
    public List<DStream<?>> dependencies()
    {
        return List$.MODULE$.<DStream<?>>newBuilder()
                .$plus$eq(parent)
                .result();
    }

    @Override
    public Duration slideDuration()
    {
        return parent.slideDuration();
    }

    @Override
    public Option<RDD<T>> compute(Time validTime)
    {
        return parent.getOrCompute(validTime);
    }

    public abstract void commitOffsets(RDD<?> kafkaRdd);

//    public abstract void commitOffsetsAsync();
}
