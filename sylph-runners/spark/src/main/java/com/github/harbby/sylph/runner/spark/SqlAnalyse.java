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
package com.github.harbby.sylph.runner.spark;

import com.github.harbby.sylph.parser.tree.CreateFunction;
import com.github.harbby.sylph.parser.tree.CreateStreamAsSelect;
import com.github.harbby.sylph.parser.tree.CreateTable;
import com.github.harbby.sylph.parser.tree.InsertInto;
import com.github.harbby.sylph.parser.tree.SelectQuery;

public interface SqlAnalyse
{
    public void finish();

    public void createStreamAsSelect(CreateStreamAsSelect statement)
            throws Exception;

    public void createTable(CreateTable statement, String className)
            throws ClassNotFoundException;

    public void createFunction(CreateFunction statement)
            throws Exception;

    public void insertInto(InsertInto statement)
            throws Exception;

    public void selectQuery(SelectQuery statement)
            throws Exception;
}
