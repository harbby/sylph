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
package ideal.sylph.plugins.kafka.flink;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.api.Source;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.guava18.com.google.common.base.Supplier;
import org.apache.flink.shaded.guava18.com.google.common.base.Suppliers;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * test source
 **/
@Name("test")
@Description("this flink test source inputStream")
@Version("1.0.0")
public class TestSource
        implements Source<DataStream<Row>>
{
    private static final long serialVersionUID = 2L;

    private final transient Supplier<DataStream<Row>> loadStream;

    public TestSource(StreamExecutionEnvironment execEnv)
    {
        this.loadStream = Suppliers.memoize(() -> execEnv.addSource(new MyDataSource()));
    }

    @Override
    public DataStream<Row> getSource()
    {
        return loadStream.get();
    }

    public static class MyDataSource
            extends RichParallelSourceFunction<Row>
            implements ResultTypeQueryable<Row>
    {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Row> sourceContext)
                throws Exception
        {
            Random random = new Random();
            int numKeys = 10;
            long count = 1L;
            while (running) {
                long eventTime = System.currentTimeMillis() - random.nextInt(10 * 1000); //表示数据已经产生了 但是会有10秒以内的延迟
                String userId = "uid_" + count;

                String msg = MAPPER.writeValueAsString(ImmutableMap.of("user_id", userId, "ip", "127.0.0.1", "store", 12.0));
                Row row = Row.of("key" + random.nextInt(10), msg, eventTime);
                sourceContext.collect(row);
                count = count > numKeys ? 1L : count + 1;
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }

        @Override
        public TypeInformation<Row> getProducedType()
        {
            TypeInformation<?>[] types = new TypeInformation<?>[] {
                    TypeExtractor.createTypeInfo(String.class),
                    TypeExtractor.createTypeInfo(String.class),
                    TypeExtractor.createTypeInfo(long.class) //createTypeInformation[String]
            };

            RowTypeInfo rowTypeInfo = new RowTypeInfo(types, new String[] {"key", "message", "event_time"});
            //createTypeInformation[Row]
            return rowTypeInfo;
        }

        @Override
        public void cancel()
        {
            running = false;
        }

        @Override
        public void close()
                throws Exception
        {
            this.cancel();
            super.close();
        }
    }
}
