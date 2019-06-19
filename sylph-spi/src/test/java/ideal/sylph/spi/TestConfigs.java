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
package ideal.sylph.spi;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import ideal.sylph.etl.Row;
import ideal.sylph.etl.api.RealTimeSink;
import ideal.sylph.etl.api.Sink;

import java.util.HashSet;
import java.util.stream.Stream;

public class TestConfigs
{
    private TestConfigs() {}

    @Name("spi_test_realtime_plugin")
    @Description("this is spi_test_plugin")
    public static class TestRealTimeSinkPlugin
            implements RealTimeSink
    {
        public TestRealTimeSinkPlugin(HashSet<String> hashSet, TestConfigs.TestDefaultValueConfig config)
        {
        }

        @Override
        public void process(Row value)
        {
        }

        @Override
        public boolean open(long partitionId, long version)
                throws Exception
        {
            return false;
        }

        @Override
        public void close(Throwable errorOrNull)
        {
        }
    }

    @Name("spi_test_sink_plugin")
    @Description("this is spi_test_plugin")
    public static class TestSinkPlugin
            implements Sink<Stream<Row>>
    {
        public TestSinkPlugin(HashSet<String> hashSet, TestConfigs.TestDefaultValueConfig config)
        {
        }

        @Override
        public void run(Stream<Row> stream)
        {
        }
    }

    @Description("this is spi_test_plugin")
    public static class TestErrorSinkPlugin
            implements Sink
    {
        public TestErrorSinkPlugin(HashSet<String> hashSet, TestConfigs.TestDefaultValueConfig config)
        {
        }

        @Override
        public void run(Object stream)
        {
        }
    }

    public static class TestDefaultValueConfig
            extends PluginConfig
    {
        @Name("name")
        private String name = "sylph";

        @Name("age")
        @Description("this is age")
        private Integer age;
    }

    public static class NoParameterConfig
            extends PluginConfig
    {
        @Name("name")
        private String name = "sylph";

        @Name("age")
        @Description()
        private int age;

        public String getName()
        {
            return name;
        }

        private NoParameterConfig(int age)
        {
            this.age = age;
        }
    }
}
