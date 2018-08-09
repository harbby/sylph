package ideal.sylph.main;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import ideal.sylph.common.bootstrap.Bootstrap;
import ideal.sylph.controller.ControllerApp;
import ideal.sylph.main.server.RunnerLoader;
import ideal.sylph.main.server.ServerMainModule;
import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.PipelinePluginLoader;
import ideal.sylph.spi.job.JobStore;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class SylphMaster
{
    private SylphMaster() {}

    private static final Logger logger = LoggerFactory.getLogger(SylphMaster.class);

    public static void main(String[] args)
    {
        PropertyConfigurator.configure(System.getProperty("log4j.file"));
        List<Module> modules = ImmutableList.of(new ServerMainModule());

        /*2 Initialize Guice Injector */
        try {
            Injector injector = new Bootstrap(modules).strictConfig().requireExplicitBindings(false).initialize();
            injector.getInstance(PipelinePluginLoader.class).loadPlugins();
            injector.getInstance(RunnerLoader.class).loadPlugins();
            injector.getInstance(JobStore.class).loadJobs();

            injector.getInstance(JobManager.class).start();
            injector.getInstance(ControllerApp.class).start();
            //ProcessHandle.current().pid()
            logger.info("======== SERVER STARTED this pid is {}========");
        }
        catch (Throwable e) {
            logger.error("SERVER START FAILED...", e);
            System.exit(1);
        }
    }
}
