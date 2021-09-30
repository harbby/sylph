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

import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.sylph.spi.dao.JobRunState;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class StatusRepositoryImpl
        implements StatusRepository<SQLException>
{
    private final RowProcessor processor = new BasicRowProcessor(new GenerousBeanProcessor());
    private final QueryRunner queryRunner;

    @Autowired
    public StatusRepositoryImpl(QueryRunner queryRunner)
    {
        this.queryRunner = queryRunner;
    }

    @Override
    public void deleteById(int jobId)
            throws SQLException
    {
        queryRunner.update("delete from running where job_id=?", jobId);
    }

    @Override
    public List<JobRunState> findAll()
            throws SQLException
    {
        return queryRunner.query("select * from running", new BeanListHandler<>(JobRunState.class, processor));
    }

    @Override
    public Optional<JobRunState> findById(int jobId)
            throws SQLException
    {
        return Optional.ofNullable(queryRunner.query("select * from running where job_id=?",
                new BeanHandler<>(JobRunState.class, processor), jobId));
    }

    @Override
    public Optional<JobRunState> findByRunId(String runId)
            throws SQLException
    {
        return Optional.ofNullable(queryRunner.query("select * from running where run_id=?",
                new BeanHandler<>(JobRunState.class, processor), runId));
    }

    @Override
    public List<JobRunState> findByState(JobRunState.Status state)
            throws SQLException
    {
        return queryRunner.query("select * from running where status=?",
                new BeanListHandler<>(JobRunState.class, processor), state);
    }

    @Override
    public int updateStateById(int jobId, JobRunState.Status states)
            throws SQLException
    {
        return queryRunner.update("""
                update running set modify_time = %s, status= '%s' where job_id=%s
                """.formatted(System.currentTimeMillis(), states.name(), jobId));
    }

    @Override
    public int updateRunIdById(int jobId, String runId, String webUi)
            throws SQLException
    {
        return queryRunner.update("update running set run_id=?,status='RUNNING',web_ui=?,modify_time=? where job_id=?",
                runId,
                webUi,
                System.currentTimeMillis(),
                jobId);
    }

    @Override
    public void save(JobRunState jobRunState)
            throws SQLException
    {
        queryRunner.update("replace into running values(?,?,?,?,?,?,?)",
                jobRunState.getJobId(),
                jobRunState.getRunId(),
                jobRunState.getRuntimeType(),
                System.currentTimeMillis(),
                jobRunState.getStatus(),
                jobRunState.getWebUi(),
                jobRunState.getType());
    }
}
