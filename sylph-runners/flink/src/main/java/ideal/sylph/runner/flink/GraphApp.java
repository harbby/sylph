package ideal.sylph.runner.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import ideal.sylph.api.NodeLoader;
import ideal.sylph.common.graph.Graph;
import ideal.sylph.common.graph.impl.DagNode;
import ideal.sylph.runner.flink.etl.FlinkPluginLoaderImpl;
import ideal.sylph.runner.flink.utils.JsonTextUtil;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.model.EdgeInfo;
import ideal.sylph.spi.model.NodeInfo;
import ideal.sylph.spi.utils.GenericTypeReference;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

public class GraphApp
        implements FlinkApp
{
    private static final transient ObjectMapper MAPPER = new ObjectMapper();
    private final Flow flow;
    private final String jobId;

    public GraphApp(String jobid, Flow flow)
    {
        this.flow = flow;
        this.jobId = jobid;
    }

    @Override
    public void build(StreamExecutionEnvironment execEnv)
            throws Exception
    {
        final Graph<DataStream<Row>> graphx = Graph.newGraph(jobId);
        List<NodeInfo> nodes = flow.getNodes();
        List<EdgeInfo> edges = flow.getEdges();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnv);
        final NodeLoader<StreamTableEnvironment, DataStream<Row>> loader = new FlinkPluginLoaderImpl();
        nodes.forEach(nodeInfo -> {
            try {
                String json = JsonTextUtil.readJsonText(nodeInfo.getNodeData());
                final Map<String, Object> config = MAPPER.readValue(json, new GenericTypeReference(Map.class, String.class, Object.class));
                String id = nodeInfo.getNodeId();

                switch (nodeInfo.getNodeType()) {
                    case "source":
                        graphx.addNode(new DagNode<>(id, loader.loadSource(tableEnv, config)));
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
        graphx.build();
    }
}
