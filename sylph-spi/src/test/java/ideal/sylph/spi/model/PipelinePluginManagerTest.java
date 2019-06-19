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

import com.github.harbby.gadtry.collection.mutable.MutableSet;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.TestConfigs;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.JobActuatorHandle;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.Throwables.throwsException;

public class PipelinePluginManagerTest
{
    private final PipelinePluginManager pluginManager = new PipelinePluginManager()
    {
        @Override
        public Set<PipelinePluginInfo> getAllPlugins()
        {
            return MutableSet.<PipelinePluginInfo>builder()
                    .add(PipelinePluginInfo.getPluginInfo(new File(""), TestConfigs.TestSinkPlugin.class))
                    .add(PipelinePluginInfo.getPluginInfo(new File(""), TestConfigs.TestRealTimeSinkPlugin.class))
                    .add(PipelinePluginInfo.getPluginInfo(new File(""), TestConfigs.TestErrorSinkPlugin.class))
                    .build();
        }
    };

    private final PipelinePluginManager defaultPm = PipelinePluginManager.getDefault();

    @Test
    public void defaultPipelinePluginManagerLoadDriver()
    {
        Class<String> stringClass = defaultPm.loadPluginDriver(String.class.getName(), null);
        Assert.assertEquals(stringClass, stringClass);

        try {
            defaultPm.loadPluginDriver("a.b.c.d", null);
            Assert.fail();
            throwsException(ClassNotFoundException.class);
        }
        catch (ClassNotFoundException e) {
            Assert.assertEquals(e.getMessage(), "a.b.c.d");
        }
    }

    @Test
    public void getAllPlugins()
    {
        Set<PipelinePluginInfo> set = PipelinePluginManager.getDefault().getAllPlugins();
        Assert.assertNotNull(set);
    }

    @Test
    public void loadPluginDriver()
    {
        String name = TestConfigs.TestRealTimeSinkPlugin.class.getAnnotation(Name.class).value();
        Class<?> aClass = pluginManager.loadPluginDriver(name, PipelinePlugin.PipelineType.sink);
        Assert.assertEquals(TestConfigs.TestRealTimeSinkPlugin.class, aClass);

        try {
            pluginManager.loadPluginDriver("a.b.c.d", PipelinePlugin.PipelineType.sink);
            Assert.fail();
            throwsException(ClassNotFoundException.class);
        }
        catch (ClassNotFoundException e) {
            Assert.assertEquals(e.getMessage(), "pipelineType:sink no such driver class: a.b.c.d");
        }
    }

    @Test
    public void filterRunnerPlugins()
    {
        Set<PipelinePluginInfo> set = PipelinePluginManager.filterRunnerPlugins(pluginManager.getAllPlugins(),
                MutableSet.of(Stream.class.getName()),
                TestRunner.class);
        Assert.assertEquals(set.size(), 2);

        Set<PipelinePluginInfo> set2 = PipelinePluginManager.filterRunnerPlugins(pluginManager.getAllPlugins(),
                MutableSet.of(), TestRunner.class);
        Assert.assertEquals(set2.size(), 1);
    }

    public static class TestRunner
            implements Runner
    {
        @Override
        public Set<JobActuatorHandle> create(RunnerContext context)
        {
            return null;
        }

        @Override
        public Class<? extends ContainerFactory> getContainerFactory()
        {
            return null;
        }
    }
}