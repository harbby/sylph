package ideal.sylph.main;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import ideal.sylph.main.controller.ControllerApp;
import ideal.sylph.main.server.PluginLoader;
import ideal.sylph.main.server.ServerMainModule;
import ideal.sylph.main.server.StaticJobLoader;
import ideal.sylph.spi.bootstrap.Bootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SylphMaster
{
    private SylphMaster() {}

    private static final Logger logger = LoggerFactory.getLogger(SylphMaster.class);

    public static void main(String[] args)
    {
        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new ServerMainModule());

        /*2 Initialize Guice Injector */
        try {
            Injector injector = new Bootstrap(modules.build()).strictConfig().initialize();
            injector.getInstance(PluginLoader.class).loadPlugins();
            injector.getInstance(StaticJobLoader.class).loadJobs();
            injector.getInstance(ControllerApp.class).start();
        }
        catch (Exception e) {
            logger.error("", e);
            e.printStackTrace();
            System.exit(1);
        }

        var pid = ProcessHandle.current().pid();
        logger.info("======== SERVER STARTED this pid is {}========", pid);
    }
}
