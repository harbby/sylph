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

import java.util.Optional;

import static java.lang.String.format;

public abstract class Expression
        extends Node
{
    protected Expression(Optional<NodeLocation> location)
    {
        super(location);
    }

    @Override
    public String toString()
    {
        throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), this.getClass().getSimpleName()));
    }

    public static Object getJavaValue(Expression node)
    {
        if (node instanceof BooleanLiteral) {
            return ((BooleanLiteral) node).getValue();
        }
        else if (node instanceof StringLiteral) {
            return ((StringLiteral) node).getValue();
        }
        else if (node instanceof LongLiteral) {
            return ((LongLiteral) node).getValue();
        }
        else if (node instanceof DoubleLiteral) {
            return ((DoubleLiteral) node).getValue();
        }
        else if (node instanceof Identifier) {
            return ((Identifier) node).getValue();
        }
        else {
            throw new UnsupportedOperationException("this Expression " + node.getClass() + " have't support!");
        }
    }
}
