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
package ideal.sylph.runner.spark;

import com.google.inject.Guice;
import com.google.inject.Injector;
import ideal.common.base.Lazys;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.runtime.yarn.YarnJobContainer;
import ideal.sylph.runtime.yarn.YarnJobContainerProxy;
import ideal.sylph.runtime.yarn.YarnModule;
import ideal.sylph.spi.job.ContainerFactory;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.Optional;

public class SparkContainerFactory
        implements ContainerFactory
{
    private final Lazys.Supplier<SparkAppLauncher> yarnLauncher = Lazys.goLazy(() -> {
        Injector injector = Guice.createInjector(new YarnModule());
        return injector.getInstance(SparkAppLauncher.class);
    });

    @Override
    public JobContainer getYarnContainer(Job job, String lastRunid)
    {
        SparkAppLauncher appLauncher = yarnLauncher.get();
        final JobContainer yarnJobContainer = new YarnJobContainer(appLauncher.getYarnClient(), lastRunid)
        {
            @Override
            public Optional<String> run()
                    throws Exception
            {
                this.setYarnAppId(null);
                ApplicationId yarnAppId = appLauncher.run(job);
                this.setYarnAppId(yarnAppId);
                return Optional.of(yarnAppId.toString());
            }
        };
        //----create JobContainer Proxy
        return YarnJobContainerProxy.get(yarnJobContainer);
    }

    @Override
    public JobContainer getLocalContainer(Job job, String lastRunid)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public JobContainer getK8sContainer(Job job, String lastRunid)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
