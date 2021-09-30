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
package com.github.harbby.sylph.parser.tree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;

public class SelectQuery
        extends Statement
{
    private final StringLiteral query;
    private final AllowedLateness allowedLateness;
    private final WindowTrigger windowTrigger;
    private final int queryEnd;
    private Map<Identifier, SelectQuery> withTableQuery = Collections.emptyMap();

    public SelectQuery(NodeLocation location,
            String query,
            int queryEnd,
            AllowedLateness allowedLateness,
            WindowTrigger windowTrigger)
    {
        super(location);
        this.query = new StringLiteral(location, query);
        this.queryEnd = queryEnd;
        this.allowedLateness = allowedLateness;
        this.windowTrigger = windowTrigger;
    }

    public void setWithTableQuery(Map<Identifier, SelectQuery> withTableQuery)
    {
        this.withTableQuery = withTableQuery;
    }

    public Optional<AllowedLateness> getAllowedLateness()
    {
        return Optional.ofNullable(allowedLateness);
    }

    public Map<Identifier, SelectQuery> getWithTableQuery()
    {
        return withTableQuery;
    }

    public Optional<WindowTrigger> getWindowTrigger()
    {
        return Optional.ofNullable(windowTrigger);
    }

    public String getQuery()
    {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<Identifier, SelectQuery> entry : withTableQuery.entrySet()) {
            builder.append(",").append(entry.getKey().getValue()).append(" AS (").append(entry.getValue().getQuery()).append(")");
        }
        String withQuery = withTableQuery.isEmpty() ? "" : "WITH " + builder.substring(1);
        return withQuery + query.getValue();
    }

    public int getQueryEndIndex()
    {
        return queryEnd;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return new ArrayList<Node>(3)
        {
            {
                this.add(query);
            }
        };
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, allowedLateness, windowTrigger);
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
        SelectQuery o = (SelectQuery) obj;
        return Objects.equals(query, o.query) &&
                Objects.equals(allowedLateness, o.allowedLateness) &&
                Objects.equals(windowTrigger, o.windowTrigger);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("query", query)
                .add("allowedLateness", allowedLateness)
                .add("windowTrigger", windowTrigger)
                .toString();
    }
}
