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

import com.github.harbby.gadtry.aop.mock.Mock;
import com.github.harbby.gadtry.aop.mock.MockGo;
import com.github.harbby.gadtry.aop.mock.MockGoJUnitRunner;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.model.NodeInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.harbby.gadtry.aop.mock.MockGo.when;
import static com.github.harbby.gadtry.aop.mock.MockGoArgument.anyMap;
import static com.github.harbby.gadtry.aop.mock.MockGoArgument.anyString;

@RunWith(MockGoJUnitRunner.class)
public class GraphAppUtilTest
{
    final String jobFlow = "---\n" +
            "nodes:\n" +
            "- nodeId: \"node1539152137911\"\n" +
            "  nodeLable: \"kafka\"\n" +
            "  nodeType: \"source\"\n" +
            "  nodeConfig:\n" +
            "    in: 0\n" +
            "    out: 1\n" +
            "    drag: 1\n" +
            "  nodeText: \"{\\n  \\\"user\\\": {\\n    \\\"kafka_group_id\\\": \\\"sylph_streamSql_test1\\\",\\n\\\n" +
            "    \\    \\\"kafka_topic\\\": \\\"test1\\\",\\n    \\\"auto.offset.reset\\\": \\\"latest\\\",\\n   \\\n" +
            "    \\ \\\"kafka_broker\\\": \\\"localhost:9092\\\"\\n  },\\n  \\\"plugin\\\": {\\n    \\\"driver\\\"\\\n" +
            "    : \\\"kafka\\\",\\n    \\\"name\\\": \\\"kafka_1539152137911\\\"\\n  }\\n}\"\n" +
            "  nodeX: 109\n" +
            "  nodeY: 64\n" +
            "- nodeId: \"node1539152139855\"\n" +
            "  nodeLable: \"TestTrans\"\n" +
            "  nodeType: \"transform\"\n" +
            "  nodeConfig:\n" +
            "    in: 1\n" +
            "    out: 1\n" +
            "    drag: 1\n" +
            "  nodeText: \"{\\n  \\\"user\\\": {},\\n  \\\"plugin\\\": {\\n    \\\"driver\\\": \\\"ideal.sylph.plugins.mysql.TestTrans\\\"\\\n" +
            "    ,\\n    \\\"name\\\": \\\"TestTrans_1539152139855\\\"\\n  }\\n}\"\n" +
            "  nodeX: 296\n" +
            "  nodeY: 96\n" +
            "- nodeId: \"node1539152140832\"\n" +
            "  nodeLable: \"PrintSink\"\n" +
            "  nodeType: \"sink\"\n" +
            "  nodeConfig:\n" +
            "    in: 1\n" +
            "    out: 0\n" +
            "    drag: 1\n" +
            "  nodeText: \"{\\n  \\\"user\\\": {},\\n  \\\"plugin\\\": {\\n    \\\"driver\\\": \\\"console\\\"\\\n" +
            "    ,\\n    \\\"name\\\": \\\"PrintSink_1539152140832\\\"\\n  }\\n}\"\n" +
            "  nodeX: 518\n" +
            "  nodeY: 134\n" +
            "edges:\n" +
            "- labelText: \"\"\n" +
            "  uuids:\n" +
            "  - \"node1539152137911-RightMiddle\"\n" +
            "  - \"node1539152139855-LeftMiddle\"\n" +
            "- labelText: \"\"\n" +
            "  uuids:\n" +
            "  - \"node1539152139855-RightMiddle\"\n" +
            "  - \"node1539152140832-LeftMiddle\"\n";

    @Mock private NodeLoader<Integer> loader;

    @Before
    public void setUp()
    {
        MockGo.initMocks(this);
    }

    @Test
    public void buildGraphReturn2()
            throws IOException
    {
        AtomicInteger sinkData = new AtomicInteger(0);
        when(loader.loadSource(anyString(), anyMap())).thenReturn((input) -> 1);
        when(loader.loadTransform(anyString(), anyMap())).thenReturn((input) -> input + 1);
        when(loader.loadSink(anyString(), anyMap())).thenReturn((input) -> {
            sinkData.set(input);
            return null;
        });

        EtlFlow flow = EtlFlow.load(jobFlow);
        GraphAppUtil.buildGraph(loader, flow);
        Assert.assertEquals(2, sinkData.get());
    }

    @Mock private NodeInfo nodeInfo;

    @Test
    public void buildGraphReturnTest()
    {
        when(nodeInfo.getNodeType()).thenReturn("test");
        EtlFlow etlFlow = new EtlFlow(Arrays.asList(nodeInfo), Collections.emptyList());
        try {
            GraphAppUtil.buildGraph(loader, etlFlow);
            Assert.fail();
        }
        catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Unknown type: nodeInfo");
        }
    }
}
