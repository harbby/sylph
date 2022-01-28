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
import com.github.harbby.sylph.main.server.ServerMainConfig;
import com.github.harbby.sylph.main.service.JobEngineManager;
import com.github.harbby.sylph.spi.dao.Job;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class JobRepositoryImpl
        implements JobRepository<SQLException>
{
    private static final Logger logger = LoggerFactory.getLogger(JobRepository.class);
    private final ServerMainConfig config;
    private final JobEngineManager runnerManger;

    //开启下划线->驼峰转换所用
    private final RowProcessor processor = new BasicRowProcessor(new GenerousBeanProcessor());
    private final QueryRunner queryRunner;

    @Autowired
    public JobRepositoryImpl(
            ServerMainConfig config,
            JobEngineManager runnerManger,
            QueryRunner queryRunner)
    {
        this.config = requireNonNull(config, "server config is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger config is null");
        this.queryRunner = queryRunner;
    }

    @Override
    public void save(Job job)
            throws SQLException
    {
        //queryRunner.insert()
        queryRunner.update("replace into jobs values(?,?,?,?,?,?)",
                job.getId(),
                job.getJobName(),
                job.getQueryText(),
                job.getType(),
                job.getConfig(),
                "files".getBytes());
    }

    @Override
    public Job getById(int jobId)
            throws SQLException
    {
        return queryRunner.query("select * from jobs where id=?", new BeanHandler<>(Job.class, processor), jobId);
    }

    @Override
    public List<Job> findAll()
            throws SQLException
    {
        return queryRunner.query("select * from jobs", new BeanListHandler<>(Job.class, processor));
    }

    @Override
    public void deleteById(int jobId)
            throws SQLException
    {
        Connection conn = queryRunner.getDataSource().getConnection();
        conn.setAutoCommit(false);
        try {
            queryRunner.update(conn, "delete from running where job_id=?", jobId);
            queryRunner.update(conn, "delete from jobs where id=?", jobId);
            conn.commit();
        }
        catch (SQLException e) {
            conn.rollback();
        }
        finally {
            conn.close();
        }
    }
}
