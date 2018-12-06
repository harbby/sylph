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
package ideal.sylph.main.server;

import com.github.harbby.gadtry.function.Creator;
import com.github.harbby.gadtry.ioc.Autowired;
import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.Binder;
import ideal.sylph.controller.ServerConfig;
import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.LocalJobStore;
import ideal.sylph.main.service.MetadataManager;
import ideal.sylph.main.service.PipelinePluginLoader;
import ideal.sylph.main.service.RunnerManager;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.job.JobStore;

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

        binder.bind(MetadataManager.class).withSingle();
        binder.bind(JobStore.class).by(LocalJobStore.class).withSingle();

        //  --- Binding parameter
        binder.bind(PipelinePluginLoader.class).withSingle();
        binder.bind(RunnerManager.class).withSingle();
        binder.bind(RunnerLoader.class).withSingle();
        binder.bind(JobManager.class).withSingle();

        binder.bind(SylphContext.class).byCreator(SylphContextProvider.class).withSingle();
    }

    private static class SylphContextProvider
            implements Creator<SylphContext>
    {
        @Autowired private JobManager jobManager;
        @Autowired private RunnerManager runnerManger;

        @Override
        public SylphContext get()
        {
            return new SylphContextImpl(jobManager, runnerManger);
        }
    }
}
