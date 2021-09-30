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
package com.github.harbby.sylph.main.dao;

import com.github.harbby.sylph.spi.dao.Job;

import java.util.List;

public interface JobRepository<E extends Throwable>
{
    public void save(Job job)
            throws E;

    public Job getById(int jobId)
            throws E;

    public void deleteById(int jobId)
            throws E;

    public List<Job> findAll() throws E;
}
