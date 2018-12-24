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
package ideal.sylph.plugins.clickhouse;

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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * test source
 **/
@Name("testCK")
@Description("this flink test source inputStream")
@Version("1.0.0")
public class TestCKSource
        implements Source<DataStream<Row>>
{
    private static final long serialVersionUID = 2L;
    private static final Logger logger = LoggerFactory.getLogger(TestCKSource.class);
    private final transient Supplier<DataStream<Row>> loadStream;

    public TestCKSource(StreamExecutionEnvironment execEnv)
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
            Random random = new Random(1000000);
            int numKeys = 10;
            while (running) {
                java.time.LocalDate date = java.time.LocalDate.now();
                java.sql.Date now = java.sql.Date.valueOf(date);
                String msg = "https://github.com/harbby/sylph/" + random.nextLong();
                Row row = Row.of("github.com" + random.nextLong(), msg, now);
                sourceContext.collect(row);
            }
        }

        @Override
        public TypeInformation<Row> getProducedType()
        {
            TypeInformation<?>[] types = new TypeInformation<?>[] {
                    TypeExtractor.createTypeInfo(String.class),
                    TypeExtractor.createTypeInfo(String.class),
                    TypeExtractor.createTypeInfo(java.sql.Date.class)
            };

            RowTypeInfo rowTypeInfo = new RowTypeInfo(types, new String[] {"key", "message", "mes_time"});
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
