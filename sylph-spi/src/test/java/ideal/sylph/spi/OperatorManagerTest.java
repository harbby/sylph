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

import com.github.harbby.gadtry.collection.MutableList;
import com.github.harbby.gadtry.collection.MutableMap;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.spi.OperatorInfo;
import com.github.harbby.sylph.spi.OperatorType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.github.harbby.gadtry.base.Throwables.throwThrowable;

public class OperatorManagerTest
{
    private final List<OperatorInfo> connectors = MutableList.<OperatorInfo>builder()
            .add(OperatorInfo.analyzePluginInfo(TestConfigs.TestRealTimeSinkPlugin.class, Collections.emptyList()))
            .add(OperatorInfo.analyzePluginInfo(TestConfigs.TestErrorSinkPlugin.class, Collections.emptyList()))
            .build();
    private final OperatorMetaData operatorMetaData = new OperatorMetaData(connectors);

    private final OperatorMetaData defaultCs = OperatorMetaData.getDefault();

    @Test
    public void getDriverClass()
    {
        List<Map<String, Object>> configs = OperatorInfo.analyzeOperatorDefaultValue(TestConfigs.TestRealTimeSinkPlugin.class);
        List<Map> mapList = Arrays.asList(MutableMap.of("key", "name", "description", "", "default", "sylph"),
                MutableMap.of("key", "age", "description", "this is age", "default", ""));
        Assert.assertEquals(configs, mapList);
    }

    @Test
    public void defaultOperatorManagerLoadDriver()
    {
        Class<String> stringClass = defaultCs.getConnectorDriver(String.class.getName(), null);
        Assert.assertEquals(stringClass, stringClass);

        try {
            defaultCs.getConnectorDriver("a.b.c.d", null);
            Assert.fail();
            throwThrowable(ClassNotFoundException.class);
        }
        catch (ClassNotFoundException e) {
            Assert.assertEquals(e.getMessage(), "a.b.c.d");
        }
    }

    @Test
    public void loadPluginDriver()
    {
        String name = TestConfigs.TestRealTimeSinkPlugin.class.getAnnotation(Name.class).value();
        Class<?> aClass = operatorMetaData.getConnectorDriver(name, OperatorType.sink);
        Assert.assertEquals(TestConfigs.TestRealTimeSinkPlugin.class, aClass);

        try {
            operatorMetaData.getConnectorDriver("a.b.c.d", OperatorType.sink);
            Assert.fail();
            throwThrowable(ClassNotFoundException.class);
        }
        catch (ClassNotFoundException e) {
            Assert.assertEquals(e.getMessage(), "a.b.c.d");
        }
    }
}