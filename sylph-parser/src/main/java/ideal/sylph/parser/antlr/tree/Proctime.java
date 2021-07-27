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

import com.github.harbby.gadtry.collection.MutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;

public class Proctime
        extends TableElement
{
    private final Identifier name;

    public Proctime(NodeLocation location, Identifier name)
    {
        this(Optional.of(location), name);
    }

    private Proctime(Optional<NodeLocation> location, Identifier name)
    {
        super(location);
        this.name = name;
    }

    public String getName()
    {
        return name.getValue();
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return MutableList.of(name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Proctime o = (Proctime) obj;
        return Objects.equals(this.name, o.name);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .toString();
    }
}
