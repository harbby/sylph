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

public class AllowedLateness
        extends Node
{
    private final LongLiteral allowedLateness;

    public AllowedLateness(NodeLocation location, String allowedLateness)
    {
        this(Optional.ofNullable(location), new LongLiteral(location, allowedLateness));
    }

    protected AllowedLateness(Optional<NodeLocation> location, LongLiteral allowedLateness)
    {
        super(location);
        this.allowedLateness = allowedLateness;
    }

    public long getAllowedLateness()
    {
        return allowedLateness.getValue();
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return MutableList.of(allowedLateness);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(allowedLateness.getValue());
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
        AllowedLateness o = (AllowedLateness) obj;
        return Objects.equals(allowedLateness.getValue(), o.allowedLateness.getValue());
    }

    @Override
    public String toString()
    {
        return String.valueOf(allowedLateness.getValue());
    }
}
