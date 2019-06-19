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

import com.github.harbby.gadtry.ioc.IocFactory;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import static ideal.sylph.spi.PluginConfigFactory.injectConfig;

public class NodeLoaderTest
{
    @Test
    public void injectConfigTest()
            throws NoSuchFieldException, IllegalAccessException
    {
        Map<String, Object> configMap = ImmutableMap.of("name", "codeTest");
        TestConfig pluginConfig = new TestConfig();
        injectConfig(pluginConfig, configMap);
        Assert.assertEquals("codeTest", pluginConfig.name);
    }

    @Test
    public void getPluginInstancePluginConfig()
    {
        final NodeLoader nodeLoader = new NodeLoader()
        {
            @Override
            public UnaryOperator loadSource(String driverStr, Map pluginConfig)
            {
                return null;
            }

            @Override
            public UnaryOperator loadTransform(String driverStr, Map pluginConfig)
            {
                return null;
            }

            @Override
            public UnaryOperator loadSink(String driverStr, Map pluginConfig)
            {
                return null;
            }

            @Override
            public IocFactory getIocFactory()
            {
                return IocFactory.create();
            }
        };

        Map<String, Object> configMap = ImmutableMap.of("name", "codeTest");
        TestConfig pluginConfig = (TestConfig) nodeLoader.getPluginInstance(TestConfig.class, configMap);
        Assert.assertEquals("codeTest", pluginConfig.name);
    }

    @Test
    public void getPluginInstanceGiveHashSet()
    {
        Map<String, Object> configMap = ImmutableMap.of("name", "codeTest");
        Set pluginConfig = NodeLoader.getPluginInstance(HashSet.class, IocFactory.create(), configMap);
        Assert.assertNotNull(pluginConfig);
    }

    @Test
    public void injectConfigNullFileTest()
            throws NoSuchFieldException, IllegalAccessException
    {
        Map<String, Object> configMap = ImmutableMap.of("age", 123);
        TestConfig pluginConfig = new TestConfig();
        injectConfig(pluginConfig, configMap);
        Assert.assertNull(pluginConfig.name);
        Assert.assertEquals(123, pluginConfig.age);
    }

    @Test
    public void injectConfigThrowIllegalArgumentException()
            throws NoSuchFieldException, IllegalAccessException
    {
        Map<String, Object> configMap = ImmutableMap.of("age", 123L);
        TestConfig pluginConfig = new TestConfig();
        injectConfig(pluginConfig, configMap);
    }

    @Test
    public void getOtherConfigTest()
            throws NoSuchFieldException, IllegalAccessException
    {
        Map<String, Object> configMap = ImmutableMap.of(
                "name", "codeTest",
                "age", 123,
                "other", 3.1415926,
                "other_host", "localhost"
        );
        PluginConfig pluginConfig = new TestConfig();
        injectConfig(pluginConfig, configMap);
        Assert.assertEquals(pluginConfig.getOtherConfig(), ImmutableMap.of("other", 3.1415926, "other_host", "localhost"));
    }

    private static class TestConfig
            extends PluginConfig
    {
        @Name("name")
        private String name;

        @Name("age")
        @Description()
        private int age;
    }
}