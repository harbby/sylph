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

import com.github.harbby.gadtry.collection.mutable.MutableMap;
import ideal.sylph.etl.PluginConfig;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PluginConfigFactoryTest
{
    @Test
    public void givePluginConfigThrowIllegalStateException()
            throws Exception
    {
        try {
            PluginConfigFactory.pluginConfigInstance(PluginConfig.class);
            Assert.fail();
        }
        catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void pluginConfigInstance()
            throws Exception
    {
        TestConfigs.NoParameterConfig testConfig = PluginConfigFactory.pluginConfigInstance(TestConfigs.NoParameterConfig.class);
        Assert.assertNull(testConfig.getName());
    }

    @Test
    public void getDriverClass()
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        List<Map<String, Object>> configs = PluginConfigFactory.getPluginConfigDefaultValues(TestConfigs.TestDefaultValueConfig.class);
        List<Map> mapList = Arrays.asList(MutableMap.of("key", "name", "description", "", "default", "sylph"),
                MutableMap.of("key", "age", "description", "this is age", "default", ""));
        Assert.assertEquals(configs, mapList);
    }
}
