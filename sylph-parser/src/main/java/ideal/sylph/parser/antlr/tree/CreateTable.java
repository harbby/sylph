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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateTable
        extends Statement
{
    public enum Type
    {
        SINK,
        SOURCE,
        BATCH;
    }

    private final QualifiedName name;
    private final List<TableElement> elements;
    private final boolean notExists;
    private final List<Property> properties;
    private final Optional<String> comment;
    private final Type type;
    private final Optional<WaterMark> watermark;

    public CreateTable(Type type,
            NodeLocation location,
            QualifiedName name,
            List<TableElement> elements,
            boolean notExists,
            List<Property> properties,
            Optional<String> comment,
            Optional<WaterMark> watermark)
    {
        this(type, Optional.of(location), name, elements, notExists, properties, comment, watermark);
    }

    private CreateTable(Type type, Optional<NodeLocation> location, QualifiedName name,
            List<TableElement> elements, boolean notExists,
            List<Property> properties, Optional<String> comment,
            Optional<WaterMark> watermark)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
        this.notExists = notExists;
        this.properties = requireNonNull(properties, "properties is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.type = requireNonNull(type, "type is null");
        this.watermark = requireNonNull(watermark, "watermark is null");
    }

    public String getName()
    {
        return name.getParts().get(name.getParts().size() - 1);
    }

    public List<TableElement> getElements()
    {
        return elements;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public Type getType()
    {
        return type;
    }

    public Optional<WaterMark> getWatermark()
    {
        return watermark;
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(elements)
                .addAll(properties)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, elements, notExists, properties, comment, type, watermark);
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
        CreateTable o = (CreateTable) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(elements, o.elements) &&
                Objects.equals(notExists, o.notExists) &&
                Objects.equals(properties, o.properties) &&
                Objects.equals(comment, o.comment) &&
                Objects.equals(type, o.type) &&
                Objects.equals(watermark, o.watermark);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("elements", elements)
                .add("notExists", notExists)
                .add("properties", properties)
                .add("comment", comment)
                .add("type", type)
                .add("watermark", watermark)
                .toString();
    }
}
