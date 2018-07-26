package ideal.sylph.common.graph;

import ideal.sylph.common.graph.impl.DefaultGraph;

public interface Graph<E>
{
    /**
     * 创建节点
     */
    void addNode(Node<E> node);

    String getName();

    /**
     * 创建边
     */
    void addEdge(String in, String out);

    void run()
            throws Exception;

    void build(boolean parallel)
            throws Exception;

    static <E> Graph<E> newGraph(String name)
    {
        return new DefaultGraph<>(name);
    }

    static Graph<Void> newDemoGraphx(String name)
    {
        return new DefaultGraph<>(name);
    }
}
