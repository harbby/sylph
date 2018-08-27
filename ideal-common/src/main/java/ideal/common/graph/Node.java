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

import java.util.Collection;

public interface Node<E>
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
