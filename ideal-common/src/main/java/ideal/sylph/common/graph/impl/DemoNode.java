package ideal.sylph.common.graph.impl;

import ideal.sylph.common.graph.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DemoNode
        implements Node<Void>
{
    private final String id;
    private List<Node<Void>> nextNodes = new ArrayList<>();

    public DemoNode(String id)
    {
        this.id = id;
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public Void getOutput()
    {
        return null;
    }

    @Override
    public Collection<Node<Void>> nextNodes()
    {
        return this.nextNodes;
    }

    @Override
    public void addNextNode(Node<Void> node)
    {
        this.nextNodes.add(node);
    }

    @Override
    public void action(Node<Void> parentNode)
    {
        if (parentNode == null) { //根节点
            System.out.println("我是: 根节点" + toString());
        }
        else {  //叶子节点
            System.out.println("我是:" + toString() + "来自:" + parentNode.toString() + "-->" + toString());
        }
    }

    @Override
    public String toString()
    {
        return "node:" + getId();
    }
}
