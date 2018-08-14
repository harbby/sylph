package ideal.sylph.spi;

import com.fasterxml.jackson.databind.ObjectMapper;
import ideal.sylph.common.graph.Graph;
import ideal.sylph.common.graph.impl.DagNode;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.model.EdgeInfo;
import ideal.sylph.spi.model.NodeInfo;
import ideal.sylph.spi.utils.GenericTypeReference;
import ideal.sylph.spi.utils.JsonTextUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

public interface GraphApp<T, R>
        extends App<T>
{
    ObjectMapper MAPPER = new ObjectMapper();

    NodeLoader<T, R> getNodeLoader();

    default Graph<R> buildGraph(String jobId, EtlFlow flow)
    {
        final T context = getContext();
        final Graph<R> graphx = Graph.newGraph(jobId);
        final List<NodeInfo> nodes = flow.getNodes();
        final List<EdgeInfo> edges = flow.getEdges();

        final NodeLoader<T, R> loader = getNodeLoader();
        nodes.forEach(nodeInfo -> {
            try {
                String json = JsonTextUtil.readJsonText(nodeInfo.getNodeText());
                final Map<String, Object> config = MAPPER.readValue(json, new GenericTypeReference(Map.class, String.class, Object.class));
                String id = nodeInfo.getNodeId();

                switch (nodeInfo.getNodeType()) {
                    case "source":
                        graphx.addNode(new DagNode<>(id, loader.loadSource(context, config)));
                        break;
                    case "transfrom":
                        graphx.addNode(new DagNode<>(id, loader.loadTransform(config)));
                        break;
                    case "sink":
                        graphx.addNode(new DagNode<>(id, loader.loadSink(config)));
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

        return graphx;
    }
}
