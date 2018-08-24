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
