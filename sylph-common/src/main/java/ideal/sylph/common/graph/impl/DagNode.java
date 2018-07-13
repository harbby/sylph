package ideal.sylph.common.graph.impl;

import ideal.sylph.common.graph.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class DagNode<T>
        implements Node<T>
{
    private final String id;
    private List<Node<T>> nextNodes = new ArrayList<>();
    private T tempData;

    private Function<T, T> function;

    public DagNode(String id, Function<T, T> function)
    {
        this.id = id;
        this.function = function;
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public T getOutput()
    {
        return tempData;
    }

    @Override
    public Collection<Node<T>> nextNodes()
    {
        return nextNodes;
    }

    @Override
    public void addNextNode(Node<T> node)
    {
        this.nextNodes.add(node);
    }

    @Override
    public void action(Node<T> parentNode)
    {
        if (parentNode == null) { //根节点
            this.tempData = function.apply(null);  //进行变换
        }
        else {  //叶子节点
            //System.out.println("我是:" + toString() + "来自:" + parentNode.toString() + "-->" + toString());
            T parentOutput = requireNonNull(parentNode.getOutput(), parentNode.getId() + " return is null");
            this.tempData = function.apply(parentOutput);  //进行变换
        }
    }
}
