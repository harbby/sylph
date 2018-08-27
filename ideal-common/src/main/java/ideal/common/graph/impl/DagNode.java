/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.common.graph.impl;

import ideal.common.graph.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

public class DagNode<T>
        implements Node<T>
{
    private final String id;
    private List<Node<T>> nextNodes = new ArrayList<>();
    private transient T outData;

    private UnaryOperator<T> nodeFunc;

    public DagNode(String id, UnaryOperator<T> nodeFunc)
    {
        this.id = requireNonNull(id, "node id is null");
        this.nodeFunc = requireNonNull(nodeFunc, "nodeFunc is null");
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public T getOutput()
    {
        return outData;
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
            this.outData = nodeFunc.apply(null);  //进行变换
        }
        else {  //叶子节点
            T parentOutput = requireNonNull(parentNode.getOutput(), parentNode.getId() + " return is null");
            this.outData = nodeFunc.apply(parentOutput);  //进行变换
        }
    }
}
