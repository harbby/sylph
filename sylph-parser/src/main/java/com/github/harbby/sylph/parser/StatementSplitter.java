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
package com.github.harbby.sylph.parser;

import com.github.harbby.gadtry.base.Strings;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class StatementSplitter
{
    //";"  or "\\G"
    private static final String REGEX = ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)(?=([^']*'[^']*')*[^']*$)";

    private final String delimiter;
    private final String sql;

    public StatementSplitter(String sql)
    {
        this.sql = requireNonNull(sql, "sql is null");
        this.delimiter = ";";
    }

    public List<Statement> getCompleteStatements()
    {
        return Stream.of(sql.split(REGEX))
                .filter(Strings::isNotBlank)
                .map(x -> new Statement(x, delimiter))
                .collect(Collectors.toList());
    }

    public static class Statement
    {
        private final String statement;
        private final String terminator;

        public Statement(String statement, String terminator)
        {
            this.statement = requireNonNull(statement, "statement is null");
            this.terminator = requireNonNull(terminator, "terminator is null");
        }

        public String statement()
        {
            return statement;
        }

        public String terminator()
        {
            return terminator;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            Statement o = (Statement) obj;
            return Objects.equals(statement, o.statement) &&
                    Objects.equals(terminator, o.terminator);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(statement, terminator);
        }

        @Override
        public String toString()
        {
            return statement + terminator;
        }
    }
}
