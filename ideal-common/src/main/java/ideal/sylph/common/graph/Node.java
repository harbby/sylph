package ideal.sylph.common.graph;

import java.io.Serializable;
import java.util.Collection;

public interface Node<E>
        extends Serializable
{
    String getId();

    /**
     * 获取当前节点的临时数据
     */
    E getOutput();

    /**
     * 获取当前节点的所有子节点
     */
    Collection<Node<E>> nextNodes();

    void addNextNode(Node<E> node);

    void action(Node<E> parentNode);
}
