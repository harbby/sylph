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
    private final Optional<String> comment;
    private final Optional<WaterMark> watermark;
    private final String viewSql;

    public CreateStreamAsSelect(
            NodeLocation location,
            QualifiedName name,
            boolean notExists,
            Optional<String> comment,
            Optional<WaterMark> watermark,
            String viewSql)
    {
        super(Optional.of(location));
        this.name = requireNonNull(name, "table is null");
        this.notExists = notExists;
        this.comment = requireNonNull(comment, "comment is null");
        this.watermark = requireNonNull(watermark, "watermark is null");
        this.viewSql = requireNonNull(viewSql, "viewSql is null");
    }

    public Optional<WaterMark> getWatermark()
    {
        return watermark;
    }

    public String getName()
    {
        return name.getParts().get(name.getParts().size() - 1);
    }

    public String getViewSql()
    {
        return viewSql;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, notExists, comment, watermark, viewSql);
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
                Objects.equals(comment, o.comment) &&
                Objects.equals(watermark, o.watermark) &&
                Objects.equals(viewSql, o.viewSql);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("notExists", notExists)
                .add("comment", comment)
                .add("watermark", watermark)
                .add("viewSql", viewSql)
                .toString();
    }
}
