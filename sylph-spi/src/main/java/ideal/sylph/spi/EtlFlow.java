package ideal.sylph.spi;

import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.model.EdgeInfo;
import ideal.sylph.spi.model.NodeInfo;

import java.util.List;

/**
 * 用来描述一个job的详细结构
 * 可以转换为多种结构(json 和source-trans-sink 和graph)
 */
public interface EtlFlow
        extends Flow
{
    List<EdgeInfo> getEdges();

    List<NodeInfo> getNodes();
}
