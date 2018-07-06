package ideal.sylph.main;

import com.google.inject.Guice;
import ideal.sylph.main.bootstrap.Bootstrap;
import ideal.sylph.main.service.ServiceModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SylphMaster
{
    private SylphMaster() {}

    private static final Logger logger = LoggerFactory.getLogger(SylphMaster.class);

    public static void main(String[] args)
            throws Exception
    {
        //PropertyConfigurator.configure("sylph-log4j.properties");
        //var properties = PropsUtil.loadProps("ysera.properties");

        /*2 Initialize Guice Injector */
        var injector = Guice.createInjector(
                new ServiceModule()
        );
        var bootstrap = injector.getInstance(Bootstrap.class).start();
        //----创建关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                bootstrap.close();
                logger.info("Shutting down Server...");
            }
            catch (Exception e) {
                logger.error("", e);
            }
        }));

        var pid = ProcessHandle.current().pid();
        logger.info("======== SERVER STARTED this pid is {}========", pid);
    }
}
