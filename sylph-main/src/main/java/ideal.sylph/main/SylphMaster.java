package ideal.sylph.main;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import ideal.sylph.controller.ControllerApp;
import ideal.sylph.main.server.PluginLoader;
import ideal.sylph.main.server.ServerMainModule;
import ideal.sylph.main.service.JobManager;
import ideal.sylph.spi.bootstrap.Bootstrap;
import ideal.sylph.spi.job.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SylphMaster
{
    private SylphMaster() {}

    private static final Logger logger = LoggerFactory.getLogger(SylphMaster.class);

    public static void main(String[] args)
    {
        var modules = ImmutableList.of(new ServerMainModule());

        /*2 Initialize Guice Injector */
        try {
            Injector injector = new Bootstrap(modules).strictConfig().requireExplicitBindings(false).initialize();
            injector.getInstance(PluginLoader.class).loadPlugins();
            injector.getInstance(JobStore.class).loadJobs();

            injector.getInstance(JobManager.class).start();
            injector.getInstance(ControllerApp.class).start();

            logger.info("======== SERVER STARTED this pid is {}========", ProcessHandle.current().pid());
        }
        catch (Exception e) {
            logger.error("", e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
