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
import ideal.sylph.spi.ConnectorStore;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.RunnerContextImpl;
import ideal.sylph.spi.TestConfigs;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.JobEngineHandle;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.Throwables.throwsException;

public class PipelinePluginManagerTest
{
    private final Set<ConnectorInfo> connectors = MutableSet.<ConnectorInfo>builder()
            .add(ConnectorInfo.getPluginInfo(TestConfigs.TestSinkPlugin.class))
            .add(ConnectorInfo.getPluginInfo(TestConfigs.TestRealTimeSinkPlugin.class))
            .add(ConnectorInfo.getPluginInfo(TestConfigs.TestErrorSinkPlugin.class))
            .build();
    private final ConnectorStore connectorStore = new ConnectorStore(connectors);

    private final ConnectorStore defaultCs = ConnectorStore.getDefault();

    @Test
    public void defaultPipelinePluginManagerLoadDriver()
    {
        Class<String> stringClass = defaultCs.getConnectorDriver(String.class.getName(), null);
        Assert.assertEquals(stringClass, stringClass);

        try {
            defaultCs.getConnectorDriver("a.b.c.d", null);
            Assert.fail();
            throwsException(ClassNotFoundException.class);
        }
        catch (ClassNotFoundException e) {
            Assert.assertEquals(e.getMessage(), "a.b.c.d");
        }
    }

    @Test
    public void loadPluginDriver()
    {
        String name = TestConfigs.TestRealTimeSinkPlugin.class.getAnnotation(Name.class).value();
        Class<?> aClass = connectorStore.getConnectorDriver(name, PipelinePlugin.PipelineType.sink);
        Assert.assertEquals(TestConfigs.TestRealTimeSinkPlugin.class, aClass);

        try {
            connectorStore.getConnectorDriver("a.b.c.d", PipelinePlugin.PipelineType.sink);
            Assert.fail();
            throwsException(ClassNotFoundException.class);
        }
        catch (ClassNotFoundException e) {
            Assert.assertEquals(e.getMessage(), "a.b.c.d");
        }
    }

    @Test
    public void filterRunnerPlugins()
    {
        RunnerContext runnerContext = new RunnerContextImpl(() -> connectors);
        ConnectorStore connectorStore = runnerContext.createConnectorStore(MutableSet.of(Stream.class),
                TestRunner.class);

        Assert.assertEquals(connectorStore.size(), 2);

        ConnectorStore connectorStore2 = runnerContext.createConnectorStore(new HashSet<>(), TestRunner.class);
        Assert.assertEquals(connectorStore2.size(), 1);
    }

    public static class TestRunner
            implements Runner
    {
        @Override
        public Set<JobEngineHandle> create(RunnerContext context)
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