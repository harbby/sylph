package ideal.sylph.main.server;

import com.google.inject.Inject;
import ideal.sylph.main.service.RunnerManger;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.utils.DirClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import static ideal.sylph.spi.exception.StandardErrorCode.LOAD_MODULE_ERROR;
import static java.util.Objects.requireNonNull;

public class PluginLoader
{
    private static final Logger logger = LoggerFactory.getLogger(PluginLoader.class);
    private final RunnerManger runnerManger;

    @Inject
    public PluginLoader(
            final RunnerManger runnerManger
    )
    {
        this.runnerManger = requireNonNull(runnerManger, "runnerManger is null");
    }

    public void loadPlugins()
    {
        File[] listFiles = requireNonNull(new File("modules").listFiles(), "modules dis is not exists");
        Stream.of(listFiles).forEach(this::loadPlugins);
    }

    public void loadPlugins(@Nonnull final File dir)
    {
        logger.info("Found module dir directory {} Try to loading the runner", dir);
        final DirClassLoader dirClassLoader = new DirClassLoader(this.getClass().getClassLoader());
        try {
            dirClassLoader.addDir(dir);
        }
        catch (MalformedURLException e) {
            throw new SylphException(LOAD_MODULE_ERROR, e);
        }
        //TODO: find runner
        final ServiceLoader<Runner> runners = ServiceLoader.load(Runner.class, dirClassLoader);
        runners.forEach(runner -> {
            runnerManger.createRunner(runner);
            logger.info("load runner {} with dir{}", runner.getClass().getName(), dir);
        });
    }
}
