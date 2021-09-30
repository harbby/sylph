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
package com.github.harbby.sylph.main.server;

import com.github.harbby.gadtry.function.Creator;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.Binder;
import com.github.harbby.sylph.colltroller.ServerConfig;
import com.github.harbby.sylph.main.dao.JobRepository;
import com.github.harbby.sylph.main.dao.JobRepositoryImpl;
import com.github.harbby.sylph.main.dao.StatusRepository;
import com.github.harbby.sylph.main.dao.StatusRepositoryImpl;
import com.github.harbby.sylph.main.service.JobEngineManager;
import com.github.harbby.sylph.main.service.JobManager;
import com.github.harbby.sylph.main.service.OperatorManager;
import com.github.harbby.sylph.spi.SylphContext;
import org.apache.commons.dbutils.QueryRunner;
import org.sqlite.SQLiteDataSource;

import java.util.Properties;

public final class SylphBean
        implements Bean
{
    private final Properties properties;

    public SylphBean(Properties properties)
    {
        this.properties = properties;
    }

    @Override
    public void configure(Binder binder)
    {
        //--- controller ---
        binder.bind(Properties.class).byInstance(properties);
        binder.bind(ServerConfig.class).withSingle();

        binder.bind(JobRepository.class).by(JobRepositoryImpl.class).withSingle();
        binder.bind(StatusRepository.class).by(StatusRepositoryImpl.class).withSingle();
        binder.bind(QueryRunner.class).byCreator(() -> {
            SQLiteDataSource dataSource = new SQLiteDataSource();
            dataSource.setUrl("jdbc:sqlite:data/data.db");
            return new QueryRunner(dataSource);
        });
        //  --- Binding parameter
        binder.bind(OperatorManager.class).withSingle();
        binder.bind(JobEngineManager.class).withSingle();
        binder.bind(JobManager.class).withSingle();

        binder.bind(SylphContext.class).byCreator(SylphContextProvider.class).withSingle();
    }

    private static class SylphContextProvider
            implements Creator<SylphContext>
    {
        @Autowired private JobManager jobManager;
        @Autowired private JobEngineManager runnerManger;
        @Autowired private OperatorManager pluginLoader;

        @Override
        public SylphContext get()
        {
            return new SylphContextImpl(jobManager, runnerManger, pluginLoader);
        }
    }
}
