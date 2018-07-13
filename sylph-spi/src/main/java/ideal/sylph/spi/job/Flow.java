package ideal.sylph.spi.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import ideal.sylph.common.base.SylphSerializable;
import ideal.sylph.spi.model.EdgeInfo;
import ideal.sylph.spi.model.NodeInfo;

import java.util.List;

/**
 * 用来描述一个job的详细结构
 * 可以转换为多种结构(json 和source-trans-sink 和graph)
 */
public interface Flow
        extends SylphSerializable
{
    List<EdgeInfo> getEdges();

    List<NodeInfo> getNodes();

    String getJobId();

    String getType();

    /**
     * 新的拖拽方式 job
     */
    String toYamlDag()
            throws JsonProcessingException;
}
