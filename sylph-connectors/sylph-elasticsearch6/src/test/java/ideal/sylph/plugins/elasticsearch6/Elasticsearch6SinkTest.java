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
package ideal.sylph.plugins.elasticsearch6;

import com.github.harbby.gadtry.aop.mock.Mock;
import com.github.harbby.gadtry.aop.mock.MockGo;
import com.github.harbby.gadtry.aop.mock.MockGoJUnitRunner;
import com.github.harbby.gadtry.ioc.IocFactory;
import com.github.harbby.gadtry.memory.UnsafeHelper;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.etl.Record;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.PluginConfigFactory;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.harbby.gadtry.aop.mock.MockGo.doAnswer;
import static com.github.harbby.gadtry.aop.mock.MockGo.doNothing;
import static com.github.harbby.gadtry.aop.mock.MockGo.doReturn;
import static com.github.harbby.gadtry.aop.mock.MockGo.doThrow;
import static com.github.harbby.gadtry.aop.mock.MockGo.when;
import static com.github.harbby.gadtry.aop.mock.MockGoArgument.any;

@RunWith(MockGoJUnitRunner.class)
public class Elasticsearch6SinkTest
{
    private final Map<String, Object> configMap = ImmutableMap.<String, Object>builder()
            .put("batchSize", 1234)
            .put("cluster_hosts", "localhost:9300,127.0.0.1:9300")
            .build();
    private final ElasticsearchSinkConfig config = PluginConfigFactory.INSTANCE
            .createPluginConfig(ElasticsearchSinkConfig.class, configMap);

    @Mock private ActionFuture actionFuture;
    private final SinkContext sinkContext = MockGo.mock(SinkContext.class);
    private final TransportClient transportClient = MockGo.spy(UnsafeHelper
            .allocateInstance(PreBuiltTransportClient.class));

    public Elasticsearch6SinkTest()
            throws Exception
    {}

    @Test
    public void createConfigTest()
    {
        Assert.assertEquals(config.getBatchSize(), 1234);
    }

    @Test
    public void createMysqlSinkByNodeLoader()
            throws Exception
    {
        IocFactory iocFactory = IocFactory.create(binder -> {
            binder.bind(SinkContext.class).byInstance(sinkContext);
        });
        Elasticsearch6Sink sink = NodeLoader.getPluginInstance(Elasticsearch6Sink.class, iocFactory, configMap);
        Assert.assertNotNull(sink);
    }

    private Elasticsearch6Sink getSink(ClientFactory factory, Schema schema)
            throws Exception
    {
        when(sinkContext.getSchema()).thenReturn(schema);
        doReturn(actionFuture).when(transportClient).bulk(any());
        doReturn(transportClient).when(transportClient).addTransportAddress(any());
        doNothing().when(transportClient).close();
        return new Elasticsearch6Sink(sinkContext, factory);
    }

    private Elasticsearch6Sink getSink(Map<String, Object> configMap, Schema schema)
            throws Exception
    {
        ElasticsearchSinkConfig config = PluginConfigFactory.INSTANCE
                .createPluginConfig(ElasticsearchSinkConfig.class, configMap);
        ClientFactory factory = MockGo.spy(new ClientFactory(config));
        doReturn(transportClient).when(factory).createClient();
        return getSink(factory, schema);
    }

    @Test
    public void check()
            throws Exception
    {
        Elasticsearch6Sink sink = getSink(configMap, Schema.newBuilder().build());
        Assert.assertTrue(sink.open(0, 0));
        sink.process(Record.of(new Object[] {"test"}));

        AtomicBoolean closed = new AtomicBoolean(false);
        doAnswer(a -> {
            closed.set(true);
            return null;
        }).when(transportClient).close();
        sink.close(null);
        Assert.assertTrue(closed.get());
    }

    @Test
    public void closeTriggerActionGetTest()
            throws Exception
    {
        List<Object> flushData = new ArrayList<>();
        Elasticsearch6Sink sink = getSink(configMap, Schema.newBuilder().build());
        doAnswer(bulkRequest -> {
            doAnswer(x -> {
                flushData.add(bulkRequest.getArgument(0));
                return true;
            }).when(actionFuture).actionGet();
            return actionFuture;
        }).when(transportClient).bulk(any());

        Assert.assertTrue(sink.open(0, 0));
        sink.process(Record.of(new Object[] {"test"}));

        Assert.assertEquals(0, flushData.size());
        sink.close(null);   //Trigger flush

        Assert.assertEquals(1, flushData.size());
        System.out.println();
    }

    @Test
    public void openErrorButMustClose()
            throws Exception
    {
        ClientFactory factory = MockGo.spy(new ClientFactory(config));
        doThrow(new RuntimeException("openError")).when(factory).createClient();

        Elasticsearch6Sink sink = getSink(factory, Schema.newBuilder().build());
        try {
            sink.open(0, 0);
            Assert.fail();
        }
        catch (RuntimeException e) {
            Assert.assertEquals(e.getMessage(), "openError");
            sink.close(e);
        }
    }
}