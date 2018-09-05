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
package ideal.sylph.etl.impl;

import ideal.sylph.etl.Collector;
import ideal.sylph.etl.Row;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ListCollector
        implements Collector<Row>
{
    private final List<Row> list;

    public ListCollector(List<Row> list)
    {
        this.list = requireNonNull(list, "list is null");
    }

    @Override
    public void collect(Row record)
    {
        list.add(record);
    }

    @Override
    public void close()
    {
    }
}
