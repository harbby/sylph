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
package ideal.sylph.main.service;

import com.github.harbby.gadtry.ioc.Autowired;
import ideal.sylph.main.server.ServerMainConfig;
import ideal.sylph.spi.job.JobStore;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import static com.github.harbby.gadtry.base.Throwables.throwsException;
import static java.util.Objects.requireNonNull;

public class SqliteDbJobStore
        implements JobStore
{
    private static final Logger logger = LoggerFactory.getLogger(JobStore.class);
    private final ServerMainConfig config;
    private final JobEngineManager runnerManger;

    //开启下划线->驼峰转换所用
    private final RowProcessor processor = new BasicRowProcessor(new GenerousBeanProcessor());

    private final QueryRunner queryRunner;
    private final DataSource dataSource;

    @Autowired
    public SqliteDbJobStore(
            ServerMainConfig config,
            JobEngineManager runnerManger
    )
    {
        this.config = requireNonNull(config, "server config is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger config is null");

        SQLiteDataSource dataSource = new SQLiteDataSource();
        dataSource.setUrl("jdbc:sqlite:data/data.db");
        this.queryRunner = new QueryRunner(dataSource);
        this.dataSource = dataSource;
    }

    @Override
    public void saveJob(DbJob job)
    {
        try {
            queryRunner.update("replace into jobs values(?,?,?,?,?,?)",
                    job.getId(),
                    job.getJobName(),
                    job.getQueryText(),
                    job.getType(),
                    job.getConfig(),
                    "files".getBytes());
        }
        catch (SQLException e) {
            throw throwsException(e);
        }
    }

    @Override
    public DbJob getJob(int jobId)
    {
        try {
            return queryRunner.query("select * from jobs where id=?", new BeanHandler<>(DbJob.class, processor), jobId);
        }
        catch (Exception e) {
            throw throwsException(e);
        }
    }

    @Override
    public Collection<DbJob> getJobs()
    {
        try {
            return queryRunner.query("select * from jobs", new BeanListHandler<>(DbJob.class, processor));
        }
        catch (Exception e) {
            throw throwsException(e);
        }
    }

    public void runJob(int jobId, String runId, String runtimeType)
            throws Exception
    {
        queryRunner.update("replace into running values(?,?,?,?)",
                jobId,
                runId,
                runtimeType,
                System.currentTimeMillis());
    }

    public void stopJob(int jobId)
            throws Exception
    {
        queryRunner.update("delete from running where job_id=?", jobId);
    }

    @Override
    public List<JobRunState> getRunningJobs()
            throws Exception
    {
        return queryRunner.query("select * from running", new BeanListHandler<>(JobRunState.class, processor));
    }

    @Override
    public void removeJob(int jobId)
            throws SQLException
    {
        Connection conn = dataSource.getConnection();
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

    @Override
    public void loadJobs()
    {
    }
}
