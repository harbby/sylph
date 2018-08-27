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
package ideal.sylph.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class WaterMark
        extends Node
{
    private final List<Identifier> identifiers;
    private final Identifier fieldName;
    private final Identifier fieldForName;
    private final Object offset;

    public WaterMark(NodeLocation location, List<Identifier> field, Object offset)
    {
        super(Optional.of(location));
        this.offset = requireNonNull(offset, "offset is null");
        this.identifiers = requireNonNull(field, "field is null");
        checkArgument(field.size() == 2, "field size must is 2,but is " + field);
        this.fieldName = field.get(0);
        this.fieldForName = field.get(1);
    }

    public String getFieldName()
    {
        return fieldName.getValue().replaceAll("`", "").replaceAll("\"", "");
    }

    public String getFieldForName()
    {
        return fieldForName.getValue().replaceAll("`", "").replaceAll("\"", "");
    }

    public Object getOffset()
    {
        return offset;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return null;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(identifiers)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identifiers, offset);
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
        return Objects.equals(identifiers, o.identifiers) &&
                Objects.equals(offset, o.offset);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("identifiers", identifiers)
                .add("offset", offset)
                .toString();
    }

    public static class SystemOffset
    {
        private final long offset;

        public SystemOffset(long offset)
        {
            this.offset = offset;
        }

        public long getOffset()
        {
            return offset;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("offset", offset)
                    .toString();
        }
    }

    public static class RowMaxOffset
    {
        private final long offset;

        public RowMaxOffset(long offset)
        {
            this.offset = offset;
        }

        public long getOffset()
        {
            return offset;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("offset", offset)
                    .toString();
        }
    }
}
