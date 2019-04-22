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
package ideal.sylph.parser.antlr.tree;

import com.github.harbby.gadtry.collection.mutable.MutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class InsertInto
        extends Statement
{
    private final String insertQuery;
    private final QualifiedName tableName;
    private final String query;

    public InsertInto(NodeLocation location, String insertQuery, QualifiedName qualifiedName, String query)
    {
        this(Optional.of(location), insertQuery, qualifiedName, query);
    }

    private InsertInto(Optional<NodeLocation> location, String insertQuery, QualifiedName qualifiedName, String query)
    {
        super(location);
        this.insertQuery = insertQuery;
        this.tableName = qualifiedName;
        this.query = query;
    }

    public String getTableName()
    {
        return tableName.getParts().get(tableName.getParts().size() - 1);
    }

    public String getQuery()
    {
        return query;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return MutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(insertQuery, tableName, query);
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
        InsertInto o = (InsertInto) obj;
        return Objects.equals(insertQuery, o.insertQuery) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(query, o.query);
    }

    @Override
    public String toString()
    {
        return insertQuery;
    }
}
