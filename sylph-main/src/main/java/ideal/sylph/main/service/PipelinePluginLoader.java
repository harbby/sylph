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
package ideal.sylph.main.service;

import com.github.harbby.gadtry.classloader.Module;
import com.github.harbby.gadtry.classloader.PluginLoader;
import com.google.common.collect.ImmutableSet;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.etl.Plugin;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.model.ConnectorInfo;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.AnnotationFormatError;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static ideal.sylph.spi.exception.StandardErrorCode.LOAD_MODULE_ERROR;
import static ideal.sylph.spi.model.ConnectorInfo.getPluginInfo;

public class PipelinePluginLoader
{
    private static final Logger logger = LoggerFactory.getLogger(PipelinePluginLoader.class);
    private PluginLoader<Plugin> loader;

    private final ConcurrentMap<String, ModuleInfo> plugins = new ConcurrentHashMap<>();

    public static class ModuleInfo
    {
        private String name;
        private Module<Plugin> module;

        private List<ConnectorInfo> infos;

        public ModuleInfo(String name, Module<Plugin> module, List<ConnectorInfo> infos)
        {
            this.name = name;
            this.module = module;
            this.infos = infos;
        }
    }

    public void loadPlugins()
            throws Exception
    {
        this.loader = PluginLoader.<Plugin>newScanner()
                .setPlugin(Plugin.class)
                .setScanDir(new File("etl-plugins"))
                .setLoadHandler(module -> {
                    logger.info("loading module {}", module.getName());
                    List<ConnectorInfo> infos = module.getPlugins().stream()
                            .flatMap(x -> x.getConnectors().stream())
                            .map(javaClass -> getPluginInfo(module.getModulePath(), javaClass))
                            .collect(Collectors.toList());

                    plugins.put(module.getName(), new ModuleInfo(module.getName(), module, infos));
                })
                .setCloseHandler(module -> {
                    plugins.remove(module.getName());
                    logger.info("remove module {}, remove connector drivers {}", module.getName(), module.getPlugins()
                            .stream().flatMap(x -> x.getConnectors().stream()).collect(Collectors.toList()));
                })
                .load();
    }

    public Set<ConnectorInfo> getPluginsInfo()
    {
        return plugins.values().stream()
                .flatMap(x -> x.infos.stream())
                .collect(Collectors.toSet());
    }

    public void reload()
    {
        logger.warn("reloading connectors...");
        loader.reload();
        logger.warn("reload connectors done.");
    }

    public void deleteModule(String moduleName)
            throws IOException
    {
        ModuleInfo moduleInfo = plugins.remove(moduleName);
        if (moduleInfo == null) {
            return;
        }
        logger.info("removing module {}", moduleName);
        FileUtils.deleteDirectory(moduleInfo.module.getModulePath());
        this.reload();
    }

    public List<Module<Plugin>> getModules()
    {
        return loader.getModules();
    }

    private static final String PREFIX = "META-INF/services/";   // copy form ServiceLoader

    @SuppressWarnings("unchecked")
    private static <T> Set<Class<? extends T>> loadPipelinePlugins(Class<T> pluginClass, ClassLoader pluginClassLoader)
            throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        final String fullName = PREFIX + pluginClass.getName();
        final Enumeration<URL> configs = pluginClassLoader.getResources(fullName);

        Method method = ServiceLoader.class.getDeclaredMethod("parse", Class.class, URL.class);
        method.setAccessible(true);
        ImmutableSet.Builder<Class<? extends T>> builder = ImmutableSet.builder();
        ServiceLoader<T> serviceLoader = ServiceLoader.load(pluginClass);
        while (configs.hasMoreElements()) {
            URL url = configs.nextElement();
            Iterator<String> iterator = (Iterator<String>) method.invoke(serviceLoader, PipelinePlugin.class, url);
            iterator.forEachRemaining(x -> {
                Class<?> javaClass = null;
                try {
                    javaClass = Class.forName(x, false, pluginClassLoader);  // pluginClassLoader.loadClass(x)
                    if (PipelinePlugin.class.isAssignableFrom(javaClass)) {
                        logger.info("Find {}:{}", pluginClass, x);
                        builder.add((Class<? extends T>) javaClass);
                    }
                    else {
                        logger.warn("UNKNOWN java class " + javaClass);
                    }
                }
                catch (AnnotationFormatError e) {
                    String errorMsg = "this scala class " + javaClass + " not getAnnotationsByType please see: https://issues.scala-lang.org/browse/SI-9529";
                    throw new SylphException(LOAD_MODULE_ERROR, errorMsg, e);
                }
                catch (Exception e) {
                    throw new SylphException(LOAD_MODULE_ERROR, e);
                }
            });
        }
        return builder.build();
    }
}
