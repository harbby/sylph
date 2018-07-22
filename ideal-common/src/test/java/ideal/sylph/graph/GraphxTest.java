package ideal.sylph.graph;

import ideal.sylph.common.graph.Graph;
import ideal.sylph.common.graph.impl.DemoNode;
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

        graph.build();
    }
}