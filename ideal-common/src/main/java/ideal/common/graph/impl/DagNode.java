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

import ideal.common.graph.GraphBuilder;
import ideal.common.graph.Node;

import java.util.function.UnaryOperator;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DagNode<T>
        extends Node<T>
{
    private final String id;
    private final String name;
    private final UnaryOperator<T> nodeFunc;

    private transient T outData;

    public DagNode(String id, String name, UnaryOperator<T> nodeFunc)
    {
        this.id = requireNonNull(id, "node id is null");
        this.name = requireNonNull(name, "node name is null");
        this.nodeFunc = requireNonNull(nodeFunc, "nodeFunc is null");
    }

    @Override
    public String getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public T getOutput()
    {
        return outData;
    }

    @Override
    public void action(Node<T> parentNode)
    {
        if (parentNode instanceof GraphBuilder.RootNode) { //根节点
            this.outData = nodeFunc.apply(null);  //进行变换
        }
        else {  //叶子节点
            T parentOutput = requireNonNull(parentNode.getOutput(), parentNode.getId() + " return is null");
            this.outData = nodeFunc.apply(parentOutput);  //进行变换
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", getId())
                .toString();
    }
}
