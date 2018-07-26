package ideal.sylph.common.graph.impl;

import ideal.sylph.common.graph.Graph;
import ideal.sylph.common.graph.Node;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 默认graph
 * 采用普通左二叉树遍历法
 * default 采用普通串行遍历(非并行)
 */
public class DefaultGraph<E>
        implements Graph<E>
{
    private final Map<String, Node<E>> nodes = new HashMap<>();
    private final Map<String, Node<E>> rootNodes = new HashMap<>();
    private final String name;

    public DefaultGraph(final String name)
    {
        this.name = name;
    }

    @Override
    public void addNode(Node<E> node)
    {
        nodes.put(node.getId(), node);
        rootNodes.put(node.getId(), node);
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void addEdge(String in, String out)
    {
        Node<E> inNode = nodes.get(in);
        Node<E> outNode = nodes.get(out);
        rootNodes.remove(outNode.getId());  //从根节点列表中删除
        inNode.addNextNode(outNode);
    }

    @Override
    public void run()
            throws Exception
    {
        System.out.println("开始寻找轨迹");
        System.out.println("根节点如下:" + rootNodes);
        serach(null, rootNodes.values(), false);
    }

    @Override
    public void build(boolean parallel)
            throws Exception
    {
        System.out.println("开始寻找轨迹");
        System.out.println("根节点如下:" + rootNodes);
        serach(null, rootNodes.values(), parallel);
    }

    private void serach(Node<E> parentNode, Collection<Node<E>> nodes, boolean parallel)
    {
        Stream<Node<E>> stream = nodes.stream();
        if (parallel) {
            stream = stream.parallel();
        }
        stream.forEach(x -> {
            x.action(parentNode);
            serach(x, x.nextNodes(), parallel);
        });
    }
}
