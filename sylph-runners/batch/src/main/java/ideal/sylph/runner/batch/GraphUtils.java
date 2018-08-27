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
package ideal.sylph.runner.batch;

import ideal.common.graph.Graph;
import ideal.common.graph.impl.DagNode;
import ideal.sylph.spi.EtlFlow;
import ideal.sylph.spi.job.Flow;

public class GraphUtils
{
    private GraphUtils() {}

    public static Graph<Boolean> getGraph(String jobId, Flow inFlow)
    {
        EtlFlow flow = (EtlFlow) inFlow;
        Graph<Boolean> graph = Graph.newGraph(jobId);
        flow.getNodes().forEach(nodeInfo -> {
            graph.addNode(new DagNode<>(nodeInfo.getNodeId(), (parentDone) -> {
                String nodeType = nodeInfo.getNodeType();  //执行引擎 hive sql or other
                if ("hiveSql".equals(nodeType)) {
                    //---exec hive sql----
                    String sql = nodeInfo.getNodeText();
                    //TODO: 执行sql
                    return true;
                }
                return false;
            }));
        });
        return graph;
    }
}
