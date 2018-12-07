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
package ideal.sylph.main.server;

import com.github.harbby.gadtry.classloader.PluginClassLoader;
import com.github.harbby.gadtry.classloader.ThreadContextClassLoader;
import com.github.harbby.gadtry.ioc.Autowired;
import com.google.common.collect.ImmutableList;
import ideal.sylph.main.service.RunnerManager;
import ideal.sylph.spi.Runner;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import static java.util.Objects.requireNonNull;

public class RunnerLoader
{
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("ideal.sylph.spi.")
            .add("com.github.harbby.gadtry")
            .add("ideal.sylph.annotation.")
            .add("ideal.sylph.etl.")  // etl api ?
            //-------------------------------------------------
            .add("com.fasterxml.jackson.annotation.")
            .add("com.fasterxml.jackson.")
            .add("org.openjdk.jol.")
            //----------test-------------
            //.add("com.google.inject.")
            .add("com.google.common.")
            .add("org.slf4j.")
            .add("org.apache.log4j.")
            .build();
    private static final Logger logger = LoggerFactory.getLogger(RunnerLoader.class);
    private final RunnerManager runnerManager;

    @Autowired
    public RunnerLoader(
            final RunnerManager runnerManager
    )
    {
        this.runnerManager = requireNonNull(runnerManager, "runnerManager is null");
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
        for (File file : FileUtils.listFiles(dir, null, true)) {
            logger.debug("    {}", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls)
    {
        ClassLoader spiLoader = getClass().getClassLoader();
        return new PluginClassLoader(urls, spiLoader, SPI_PACKAGES);
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
            runnerManager.createRunner(runner);
        }
    }
}
