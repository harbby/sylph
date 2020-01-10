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
package ideal.sylph.runner.flink.resource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.Type;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TestDataSource
        extends RichParallelSourceFunction<Tuple3<String, String, Long>>
        implements ResultTypeQueryable<Tuple3<String, String, Long>>
{

    volatile private boolean running = true;

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> sourceContext)
            throws Exception
    {
        final Random random = new Random();
        long numElements = 20000000;
        long numKeys = 10;
        long value = 1L;
        long count = 0L;

        while (running && count < numElements) {
            long startTime = System.currentTimeMillis() - random.nextInt(10_000);  //表示数据已经产生了 但是会有10秒以内的延迟
            sourceContext.collect(new Tuple3<>("topic" + random.nextInt(10), "uid_" + value, startTime));

            count += 1;
            value += 1;

            if (value > numKeys) {
                value = 1L;
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }
    }

    @Override
    public void cancel()
    {
        running = false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeInformation<Tuple3<String, String, Long>> getProducedType()
    {
        Type type = ParameterizedTypeImpl.make(Tuple3.class, new Type[] {String.class, String.class, Long.class}, null);

        return (TypeInformation<Tuple3<String, String, Long>>) TypeExtractor.createTypeInfo(type);
    }
}