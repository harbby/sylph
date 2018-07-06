package ideal.sylph.main.bootstrap;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import ideal.sylph.main.SylphContext;
import ideal.sylph.main.service.BatchJobManager;
import ideal.sylph.main.service.StreamJobManager;
import ideal.sylph.spi.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class Bootstrap
{
    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    private final StreamJobManager streamJobManager;
    private final BatchJobManager batchJobManager;
    private final List<Service> services;

    @Inject
    public Bootstrap(
            StreamJobManager streamJobManager,
            BatchJobManager batchJobManager)
    {
        this.streamJobManager = requireNonNull(streamJobManager, "streamJobManager is null");
        this.batchJobManager = requireNonNull(batchJobManager, "batchJobManager is null");

        this.services = ImmutableList.<Service>builder()
                .add(streamJobManager)
                .add(batchJobManager)
                .build();
    }

    public Bootstrap start()
            throws Exception
    {
        for (Service service : services) {
            logger.info("Starting service {}", service.getClass());
            service.start();
        }
        SylphContext sylphContext;

        return this;
    }

    public void close()
            throws Exception
    {
        services.forEach(service -> {
            try {
                service.close();
            }
            catch (Exception e) {
                logger.error("Stopping service {} error", service.getClass());
            }
        });
    }
}
