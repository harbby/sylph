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

import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WaterMark
        extends Node
{
    private final Identifier rowTimeName;
    private final Identifier fieldName;
    private final long offset;

    public WaterMark(NodeLocation location, Identifier rowTimeName, Identifier field, long offset)
    {
        super(location);
        this.offset = offset;
        this.rowTimeName = requireNonNull(rowTimeName, "rowTimeName is null");
        this.fieldName = requireNonNull(field, "field is null");
    }

    public String getRowTimeName()
    {
        return rowTimeName.getValue().replaceAll("`", "").replaceAll("\"", "");
    }

    public String getFieldName()
    {
        return fieldName.getValue().replaceAll("`", "").replaceAll("\"", "");
    }

    /**
     * get the offset unit ms
     *
     * @return long ms
     */
    public long getOffset()
    {
        return offset;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(fieldName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fieldName, offset);
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
        WaterMark o = (WaterMark) obj;
        return Objects.equals(fieldName, o.fieldName) &&
                Objects.equals(offset, o.offset);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fieldName", fieldName)
                .add("offset", offset)
                .toString();
    }
}
