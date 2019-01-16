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

import com.github.harbby.gadtry.graph.Graph;
import com.github.harbby.gadtry.graph.GraphBuilder;
import com.github.harbby.gadtry.graph.impl.DagNode;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.model.EdgeInfo;
import ideal.sylph.spi.model.NodeInfo;

import java.util.List;
import java.util.Map;

public class GraphAppUtil
{
    private GraphAppUtil() {}

    public static <R> Graph<R> buildGraph(final NodeLoader<R> loader, String jobId, EtlFlow flow)
    {
        final GraphBuilder<R> graph = Graph.<R>builder().name(jobId);
        final List<NodeInfo> nodes = flow.getNodes();
        final List<EdgeInfo> edges = flow.getEdges();

        nodes.forEach(nodeInfo -> {
            final Map<String, Object> config = nodeInfo.getUserConfig();
            String driverString = nodeInfo.getDriverClass();
            String id = nodeInfo.getNodeId();

            switch (nodeInfo.getNodeType()) {
                case "source":
                    graph.addNode(new DagNode<>(id, driverString, loader.loadSource(driverString, config)));
                    break;
                case "transform":
                    graph.addNode(new DagNode<>(id, driverString, loader.loadTransform(driverString, config)));
                    break;
                case "sink":
                    graph.addNode(new DagNode<>(id, driverString, loader.loadSink(driverString, config)));
                    break;
                default:
                    System.out.println("错误的类型算子 + " + nodeInfo);
            }
        });

        edges.forEach(edgeInfo -> graph.addEdge(
                edgeInfo.getInNodeId().split("-")[0],
                edgeInfo.getOutNodeId().split("-")[0]
        ));

        return graph.build();
    }
}
