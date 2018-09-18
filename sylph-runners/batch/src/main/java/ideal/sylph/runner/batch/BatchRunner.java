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
package ideal.sylph.runner.batch;

import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.common.bootstrap.Bootstrap;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.JobActuatorHandle;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class BatchRunner
        implements Runner
{
    @Override
    public Set<JobActuatorHandle> create(RunnerContext context)
    {
        requireNonNull(context, "context is null");
        try {
            Bootstrap app = new Bootstrap(binder -> {
                binder.bind(BatchEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(Scheduler.class).toProvider(this::getBatchJobScheduler).in(Scopes.SINGLETON);
            });
            Injector injector = app.strictConfig()
                    .name(this.getClass().getSimpleName())
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();

            return Sets.newHashSet(injector.getInstance(BatchEtlActuator.class));
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private Scheduler getBatchJobScheduler()
    {
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        try {
            Scheduler scheduler = schedulerFactory.getScheduler();
            scheduler.start();
            return scheduler;
        }
        catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }
}
