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
package ideal.sylph.spi.model;

import com.github.harbby.gadtry.collection.mutable.MutableMap;
import ideal.sylph.spi.TestConfigs;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConnectorInfoTest
{
    @Test
    public void getDriverClass()
    {
        List<Map<String, Object>> configs = ConnectorInfo.getConnectorDefaultConfig(TestConfigs.TestRealTimeSinkPlugin.class);
        List<Map> mapList = Arrays.asList(MutableMap.of("key", "name", "description", "", "default", "sylph"),
                MutableMap.of("key", "age", "description", "this is age", "default", ""));
        Assert.assertEquals(configs, mapList);
    }

    @Test
    public void getRealTime()
    {
    }

    @Test
    public void getNames()
    {
    }

    @Test
    public void getPluginFile()
    {
    }

    @Test
    public void getJavaGenerics()
    {
    }

    @Test
    public void getDescription()
    {
    }

    @Test
    public void getVersion()
    {
    }

    @Test
    public void getPipelineType()
    {
    }

    @Test
    public void getPluginConfig()
    {
    }

    @Test
    public void setPluginConfig()
    {
    }

    @Test
    public void hashCode1()
    {
    }

    @Test
    public void equals1()
    {
    }

    @Test
    public void toString1()
    {
    }

    @Test
    public void parserPluginDefualtConfig()
    {
    }
}