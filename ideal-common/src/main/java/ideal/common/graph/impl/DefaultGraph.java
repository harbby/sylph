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

import com.google.common.collect.ImmutableList;
import ideal.common.graph.Graph;
import ideal.common.graph.Node;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * 默认graph
 * 采用普通左二叉树遍历法
 * default 采用普通串行遍历(非并行)
 */
public class DefaultGraph<E>
        implements Graph<E>
{
    private final Node<E> root;
    private final String name;

    public DefaultGraph(
            final String name,
            Node<E> root)
    {
        this.name = name;
        this.root = root;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void run()
    {
        this.run(false);
    }

    @Override
    public void run(boolean parallel)
    {
        System.out.println("Traversing the entire graph from the root node...");
        this.show();
        serach(root, parallel);
    }

    private void serach(Node<E> node, boolean parallel)
    {
        Collection<Node<E>> nodes = node.nextNodes();
        Stream<Node<E>> stream = nodes.stream();
        if (parallel) {
            stream = stream.parallel();
        }
        stream.forEach(x -> {
            x.action(node);
            serach(x, parallel);
        });
    }

    @Override
    public void show()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("/");
        show(builder, ImmutableList.copyOf(root.nextNodes()), "├");
        builder.build().forEach(System.out::println);
    }

    private void show(ImmutableList.Builder<String> builder, List<Node<E>> nodes, String header)
    {
        for (int i = 0; i < nodes.size(); i++) {
            Node<E> node = nodes.get(i);

            if (i == nodes.size() - 1) {  //end
                header = header.substring(0, header.length() - 1) + "└";
            }
            String name = node.getId() + "[" + node.getName() + "]";
            String line = header + "────" + name;
            builder.add(line);

            String f = (node.nextNodes().size() > 1) ? "├" : "└";
            show(builder, node.nextNodes(), getNextLineHeader(line, name) + f);
        }
    }

    private static String getNextLineHeader(String lastLine, String id)
    {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < lastLine.length() - id.length(); i++) {
            char a1 = lastLine.charAt(i);
            switch (a1) {
                case '├':
                case '│':
                    buffer.append("│");
                    break;
                default:
                    buffer.append(" ");
            }
        }
        return buffer.toString();
    }
}
