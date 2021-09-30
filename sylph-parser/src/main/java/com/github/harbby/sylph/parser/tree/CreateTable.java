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

import com.github.harbby.gadtry.collection.MutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;
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
    private final List<ColumnDefinition> elements;
    private final Proctime proctime;
    private final boolean notExists;
    private final List<Property> properties;
    private final String comment;
    private final Type type;
    private final WaterMark watermark;
    private final Map<String, Object> withProperties;
    private final String connector;

    public CreateTable(Type type,
            NodeLocation location,
            QualifiedName name,
            List<ColumnDefinition> elements,
            List<Proctime> proctimeList,
            boolean notExists,
            List<Property> properties,
            String comment,
            WaterMark watermark)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.elements = MutableList.copy(requireNonNull(elements, "elements is null"));
        this.notExists = notExists;
        this.properties = requireNonNull(properties, "properties is null");
        this.comment = comment;
        this.type = requireNonNull(type, "type is null");
        this.watermark = watermark;

        checkState(proctimeList.size() <= 1, "proctime as PROCTIME() can only be one");
        proctime = proctimeList.isEmpty() ? null : proctimeList.get(0);
        this.withProperties = this.properties.stream()
                .collect(Collectors.toMap(
                        k -> k.getName(),
                        v -> Expression.getJavaValue(v.getValue())));

        this.connector = (String) this.withProperties.remove("connector");
        checkState(connector != null, "the create table must be with(connector = '...')");
    }

    public String getName()
    {
        return name.getParts().get(name.getParts().size() - 1);
    }

    public List<ColumnDefinition> getElements()
    {
        return elements;
    }

    public Optional<Proctime> getProctimes()
    {
        return Optional.ofNullable(proctime);
    }

    public boolean isCheckNotExists()
    {
        return notExists;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public String getConnector()
    {
        return connector;
    }

    public Map<String, Object> getWithProperties()
    {
        return new HashMap<>(withProperties);
    }

    public Optional<String> getComment()
    {
        return Optional.ofNullable(comment);
    }

    public Type getType()
    {
        return type;
    }

    public Optional<WaterMark> getWatermark()
    {
        return Optional.ofNullable(watermark);
    }

    @Override
    public List<Node> getChildren()
    {
        return MutableList.<Node>builder()
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
