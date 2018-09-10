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

import com.fasterxml.jackson.databind.ObjectMapper;
import ideal.common.graph.Graph;
import ideal.common.graph.GraphBuilder;
import ideal.common.graph.impl.DagNode;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.model.EdgeInfo;
import ideal.sylph.spi.model.NodeInfo;
import ideal.sylph.spi.utils.GenericTypeReference;
import ideal.sylph.spi.utils.JsonTextUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static java.util.Objects.requireNonNull;

public interface GraphApp<T, R>
        extends App<T>
{
    ObjectMapper MAPPER = new ObjectMapper();

    NodeLoader<T, R> getNodeLoader();

    default Graph<R> buildGraph(String jobId, EtlFlow flow)
    {
        final T context = getContext();
        final GraphBuilder<R> graphx = Graph.<R>builder().name(jobId);
        final List<NodeInfo> nodes = flow.getNodes();
        final List<EdgeInfo> edges = flow.getEdges();

        final NodeLoader<T, R> loader = getNodeLoader();
        nodes.forEach(nodeInfo -> {
            try {
                String json = JsonTextUtil.readJsonText(nodeInfo.getNodeText());
                final Map<String, Object> config = MAPPER.readValue(json, new GenericTypeReference(Map.class, String.class, Object.class));

                String driverString = (String) requireNonNull(config.get("driver"), "driver class IS NULL");
                String id = nodeInfo.getNodeId();

                switch (nodeInfo.getNodeType()) {
                    case "source":
                        graphx.addNode(new DagNode<>(id, driverString, loader.loadSource(context, config)));
                        break;
                    case "transfrom":
                        graphx.addNode(new DagNode<>(id, driverString, loader.loadTransform(config)));
                        break;
                    case "sink":
                        graphx.addNode(new DagNode<>(id, driverString, loader.loadSink(config)));
                        break;
                    default:
                        System.out.println("错误的类型算子");
                }
            }
            catch (IOException e) {
                throw new SylphException(JOB_BUILD_ERROR, "graph creating fail", e);
            }
        });

        //TODO:  .split("-")[0] 目前是为了兼容yaml中的冗余信息
        edges.forEach(edgeInfo -> graphx.addEdge(
                edgeInfo.getInNodeId().split("-")[0],
                edgeInfo.getOutNodeId().split("-")[0]
        ));

        return graphx.build();
    }
}
