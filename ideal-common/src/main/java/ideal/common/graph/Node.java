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

import java.util.ArrayList;
import java.util.List;

public abstract class Node<E>
{
    private List<Node<E>> nextNodes = new ArrayList<>();

    public abstract String getId();

    public abstract String getName();

    /**
     * 获取当前节点的临时数据
     */
    public abstract E getOutput();

    public abstract void action(Node<E> parentNode);

    /**
     * 获取当前节点的所有子节点
     */
    public List<Node<E>> nextNodes()
    {
        return nextNodes;
    }

    public void addNextNode(Node<E> node)
    {
        this.nextNodes.add(node);
    }

    @Override
    public abstract String toString();
}
