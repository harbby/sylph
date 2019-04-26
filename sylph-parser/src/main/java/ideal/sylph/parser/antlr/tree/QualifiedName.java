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

import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.mutable.MutableList;

import java.util.List;
import java.util.Optional;

import static com.github.harbby.gadtry.base.Iterators.isEmpty;
import static com.github.harbby.gadtry.base.Iterators.map;
import static com.github.harbby.gadtry.base.MoreObjects.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QualifiedName
{
    private final List<String> parts;
    private final List<String> originalParts;

    public static QualifiedName of(String first, String... rest)
    {
        requireNonNull(first, "first is null");

        return of(MutableList.<String>builder().add(first).addAll(rest).build());
    }

    public static QualifiedName of(String name)
    {
        requireNonNull(name, "name is null");
        return of(MutableList.of(name));
    }

    public static QualifiedName of(Iterable<String> originalParts)
    {
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");
        List<String> parts = MutableList.copy(map(originalParts, part -> part.toLowerCase(ENGLISH)));

        return new QualifiedName(MutableList.copy(originalParts), parts);
    }

    private QualifiedName(List<String> originalParts, List<String> parts)
    {
        this.originalParts = originalParts;
        this.parts = parts;
    }

    public List<String> getParts()
    {
        return parts;
    }

    public List<String> getOriginalParts()
    {
        return originalParts;
    }

    @Override
    public String toString()
    {
        return String.join(".", parts);
    }

    /**
     * For an identifier of the form "a.b.c.d", returns "a.b.c"
     * For an identifier of the form "a", returns absent
     */
    public Optional<QualifiedName> getPrefix()
    {
        if (parts.size() == 1) {
            return Optional.empty();
        }

        List<String> subList = parts.subList(0, parts.size() - 1);
        return Optional.of(new QualifiedName(subList, subList));
    }

    public boolean hasSuffix(QualifiedName suffix)
    {
        if (parts.size() < suffix.getParts().size()) {
            return false;
        }

        int start = parts.size() - suffix.getParts().size();

        return parts.subList(start, parts.size()).equals(suffix.getParts());
    }

    public String getSuffix()
    {
        return Iterators.getLast(parts);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return parts.equals(((QualifiedName) o).parts);
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }
}
