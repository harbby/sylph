package ideal.sylph.runner.batch;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.sylph.common.bootstrap.Bootstrap;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.job.JobActuator;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class BatchRunner
        implements Runner
{
    @Override
    public Set<JobActuator> create(RunnerContext context)
    {
        requireNonNull(context, "context is null");
        try {
            Bootstrap app = new Bootstrap(binder -> {
                binder.bind(BatchEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(Scheduler.class).toProvider(this::getBatchJobScheduler).in(Scopes.SINGLETON);
            });
            Injector injector = app.strictConfig()
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();
            return ImmutableSet.of(BatchEtlActuator.class
            ).stream().map(injector::getInstance).collect(Collectors.toSet());
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
