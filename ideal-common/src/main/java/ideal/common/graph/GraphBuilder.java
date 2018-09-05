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
package ideal.common.graph;

import ideal.common.graph.impl.DefaultGraph;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GraphBuilder<E>
{
    private final Map<String, Node<E>> rootNodes = new HashMap<>();
    private final Map<String, Node<E>> nodes = new HashMap<>();
    private String name;

    public GraphBuilder<E> name(String name)
    {
        this.name = name;
        return this;
    }

    public GraphBuilder<E> addNode(Node<E> node)
    {
        nodes.put(node.getId(), node);
        rootNodes.put(node.getId(), node);
        return this;
    }

    public GraphBuilder<E> addEdge(Node<E> inNode, Node<E> outNode)
    {
        inNode.addNextNode(outNode);
        rootNodes.remove(outNode.getId());  //从根节点列表中删除
        return this;
    }

    public GraphBuilder<E> addEdge(String node1, String node2)
    {
        Node<E> inNode = requireNonNull(nodes.get(node1), "Unable to create edge because " + node1 + " does not exist");
        Node<E> outNode = requireNonNull(nodes.get(node2), "Unable to create edge because " + node2 + " does not exist");

        return addEdge(inNode, outNode);
    }

    public Graph<E> build()
    {
        final Node<E> root = new RootNode<>();
        rootNodes.values().forEach(root::addNextNode);

        return new DefaultGraph<>(name, root);
    }

    public static class RootNode<E>
            extends Node<E>
    {
        @Override
        public String getId()
        {
            return "/";
        }

        @Override
        public String getName()
        {
            return getId();
        }

        @Override
        public E getOutput()
        {
            return null;
        }

        @Override
        public void action(Node<E> parentNode)
        {
        }

        @Override
        public String toString()
        {
            return toStringHelper(RootNode.this)
                    .add("id", getId())
                    .toString();
        }
    }
}
