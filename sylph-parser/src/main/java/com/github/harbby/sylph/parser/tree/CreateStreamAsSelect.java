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

import com.github.harbby.gadtry.collection.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateStreamAsSelect
        extends Statement
{
    private final QualifiedName name;
    private final boolean notExists;
    private final WaterMark watermark;
    private final String viewSql;

    public CreateStreamAsSelect(
            NodeLocation location,
            QualifiedName name,
            boolean notExists,
            WaterMark watermark,
            String viewSql)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.notExists = notExists;
        this.watermark = watermark;
        this.viewSql = requireNonNull(viewSql, "viewSql is null");
    }

    public Optional<WaterMark> getWatermark()
    {
        return Optional.ofNullable(watermark);
    }

    public String getName()
    {
        return name.getParts().get(name.getParts().size() - 1);
    }

    public String getViewSql()
    {
        return viewSql;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, notExists, watermark, viewSql);
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
        CreateStreamAsSelect o = (CreateStreamAsSelect) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(notExists, o.notExists) &&
                Objects.equals(watermark, o.watermark) &&
                Objects.equals(viewSql, o.viewSql);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("notExists", notExists)
                .add("watermark", watermark)
                .add("viewSql", viewSql)
                .toString();
    }
}
