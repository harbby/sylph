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
package com.github.harbby.sylph.spi.job;

import com.github.harbby.sylph.parser.AntlrSqlParser;
import com.github.harbby.sylph.parser.tree.CreateTable;
import com.github.harbby.sylph.parser.tree.Statement;
import com.github.harbby.sylph.spi.OperatorType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SqlJobParser
        extends JobParser
{
    public static final String SQL_REGEX = ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)(?=([^']*'[^']*')*[^']*$)";
    private final String sqlText;
    private final List<StatementNode> tree = new ArrayList<>();
    private final Set<DependOperator> dependOperators;

    public SqlJobParser(String sqlText, List<StatementNode> tree, Set<DependOperator> dependOperators)
    {
        this.sqlText = sqlText;
        this.tree.addAll(tree);
        this.dependOperators = new HashSet<>(dependOperators);
    }

    public List<StatementNode> getTree()
    {
        return tree;
    }

    public String getSqlText()
    {
        return sqlText;
    }

    @Override
    public Set<DependOperator> getDependOperators()
    {
        return dependOperators;
    }

    public static SqlJobParser parser(String sqlText)
    {
        Set<DependOperator> dependOperators = new HashSet<>();
        AntlrSqlParser parser = new AntlrSqlParser();
        List<StatementNode> treeList = new ArrayList<>();
        for (String query : sqlText.split(SQL_REGEX)) {
            if (query.trim().length() == 0) {
                continue;
            }
            Statement statement = parser.createStatement(query);

            if (statement instanceof CreateTable) {
                CreateTable createTable = (CreateTable) statement;
                String driverOrName = createTable.getConnector();
                DependOperator dependOperator = DependOperator.of(driverOrName, getPipeType(createTable.getType()));
                dependOperators.add(dependOperator);
                treeList.add(new StatementNode(statement, dependOperator));
            }
            else {
                treeList.add(new StatementNode(statement, null));
            }
        }
        return new SqlJobParser(sqlText, treeList, dependOperators);
    }

    public static class StatementNode
            implements Serializable
    {
        private final Statement statement;
        private final DependOperator dependOperator;

        public StatementNode(Statement statement, DependOperator dependOperator)
        {
            this.statement = statement;
            this.dependOperator = dependOperator;
        }

        public Statement getStatement()
        {
            return statement;
        }

        public DependOperator getDependOperator()
        {
            return dependOperator;
        }
    }

    @Override
    public String toString()
    {
        return sqlText;
    }

    private static OperatorType getPipeType(CreateTable.Type type)
    {
        switch (type) {
            case BATCH:
                throw new UnsupportedOperationException("not support batch table");
            case SINK:
                return OperatorType.sink;
            case SOURCE:
                return OperatorType.source;
            default:
                throw new IllegalArgumentException("this type " + type + " have't support!");
        }
    }
}
