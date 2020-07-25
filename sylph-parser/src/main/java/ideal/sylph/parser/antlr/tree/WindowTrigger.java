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

public class WindowTrigger
        extends Node
{
    public final LongLiteral cycleTime;

    public WindowTrigger(NodeLocation location, String cycleTime)
    {
        this(Optional.ofNullable(location), new LongLiteral(location, cycleTime));
    }

    protected WindowTrigger(Optional<NodeLocation> location, LongLiteral cycleTime)
    {
        super(location);
        this.cycleTime = cycleTime;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return MutableList.of(cycleTime);
    }

    public long getCycleTime()
    {
        return cycleTime.getValue();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cycleTime.getValue());
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
        WindowTrigger o = (WindowTrigger) obj;
        return Objects.equals(cycleTime.getValue(), o.cycleTime.getValue());
    }

    @Override
    public String toString()
    {
        return String.valueOf(cycleTime.getValue());
    }
}
