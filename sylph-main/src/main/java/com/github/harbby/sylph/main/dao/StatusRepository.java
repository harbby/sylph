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

import com.github.harbby.sylph.spi.dao.JobRunState;

import java.util.List;
import java.util.Optional;

public interface StatusRepository<E extends Throwable>
{
    public void save(JobRunState jobRunState)
            throws E;

    public int updateStateById(int jobId, JobRunState.Status state)
            throws E;

    public int updateRunIdById(int jobId, String appId, String webUi)
            throws E;

    public void deleteById(int jobId)
            throws E;

    public List<JobRunState> findAll()
            throws E;

    public List<JobRunState> findByState(JobRunState.Status state)
            throws E;

    Optional<JobRunState> findById(int jobId)
            throws E;

    Optional<JobRunState> findByRunId(String runId)
            throws E;
}
