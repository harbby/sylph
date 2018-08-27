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
package ideal.sylph.graph;

import ideal.common.graph.Graph;
import ideal.common.graph.impl.DemoNode;
import org.junit.Test;

public class GraphxTest
{
    @Test
    public void test1()
            throws Exception
    {
        Graph<Void> graph = Graph.newDemoGraphx("test1");
        graph.addNode(new DemoNode("a1"));
        graph.addNode(new DemoNode("a0"));
        graph.addNode(new DemoNode("a2"));
        graph.addNode(new DemoNode("a3"));

        graph.addEdge("a1", "a2");
        graph.addEdge("a1", "a3");
        graph.addEdge("a0", "a3");
        //-----------------------------------------
        graph.addNode(new DemoNode("a4"));
        graph.addNode(new DemoNode("a5"));
        graph.addNode(new DemoNode("a6"));

        graph.addEdge("a2", "a4");
        graph.addEdge("a2", "a5");
        graph.addEdge("a3", "a6");

        graph.run();
    }
}