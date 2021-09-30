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
package com.github.harbby.sylph.runner.spark.sparkstreaming;

import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.dstream.DStream;

public class DStreamUtil
{
    private DStreamUtil() {}

    public static DStream<?> getFirstDStream(DStream<?> stream)
    {
        return getFirstDStream(stream, null);
    }

    public static DStream<?> getFirstDStream(DStream<?> stream, Class<? extends DStream> first)
    {
        if (first != null && first.isInstance(stream)) {
            return stream;
        }
        if (stream.dependencies().isEmpty()) {
            return stream;
        }
        else {
            return getFirstDStream(stream.dependencies().head(), first);
        }
    }

    public static RDD<?> getFirstRdd(RDD<?> rdd)
    {
        if (rdd.dependencies().isEmpty()) {
            return rdd;
        }
        else {
            return getFirstRdd(rdd.dependencies().head().rdd());
        }
    }
}
