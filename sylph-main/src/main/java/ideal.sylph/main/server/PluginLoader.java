package ideal.sylph.main.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import ideal.sylph.main.service.RunnerManger;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.classloader.ThreadContextClassLoader;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

import static java.util.Objects.requireNonNull;

public class PluginLoader
{
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("ideal.sylph.spi.")
            .add("ideal.sylph.common.")
            .add("com.fasterxml.jackson.annotation.")
            .add("com.fasterxml.jackson.")
            .add("org.openjdk.jol.")
            //----------test-------------
            .add("com.google.inject.")
            .add("com.google.common.")
            .add("org.slf4j.")
            .add("org.apache.log4j.")
            .build();
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
            throws IOException
    {
        File[] listFiles = requireNonNull(new File("modules").listFiles(), "modules dir is not exists");
        for (File dir : listFiles) {
            this.loadPlugins(dir);
        }
    }

    private void loadPlugins(final File dir)
            throws IOException
    {
        logger.info("Found module dir directory {} Try to loading the runner", dir);
        URLClassLoader pluginClassLoader = buildClassLoaderFromDirectory(dir);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader);
        }
    }

    private URLClassLoader buildClassLoaderFromDirectory(File dir)
            throws IOException
    {
        logger.debug("Classpath for {}:", dir.getName());
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            logger.debug("    {}", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls)
    {
        ClassLoader parent = getClass().getClassLoader();
        return new PluginClassLoader(urls, parent, SPI_PACKAGES);
    }

    private static Collection<File> listFiles(File installedPluginsDir)
    {
        return FileUtils.listFiles(installedPluginsDir, null, true);
    }

    private void loadPlugin(URLClassLoader pluginClassLoader)
    {
        ServiceLoader<Runner> serviceLoader = ServiceLoader.load(Runner.class, pluginClassLoader);
        List<Runner> plugins = ImmutableList.copyOf(serviceLoader);

        if (plugins.isEmpty()) {
            logger.warn("No service providers of type {}", Runner.class.getName());
        }

        for (Runner runner : plugins) {
            logger.info("Installing runner {} with dir{}", runner.getClass().getName(), runner);
            runnerManger.createRunner(runner);
        }
    }
}
