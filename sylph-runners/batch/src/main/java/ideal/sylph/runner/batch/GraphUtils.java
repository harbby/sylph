package ideal.sylph.runner.batch;

import ideal.sylph.common.graph.Graph;
import ideal.sylph.common.graph.impl.DagNode;
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
