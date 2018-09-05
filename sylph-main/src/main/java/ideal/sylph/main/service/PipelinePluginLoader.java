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

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableSet;
import ideal.common.classloader.DirClassLoader;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.annotation.Version;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.etl.api.RealTimePipeline;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.repository.AbstractRepository;
import sun.reflect.generics.repository.ClassRepository;
import sun.reflect.generics.tree.ClassSignature;
import sun.reflect.generics.tree.ClassTypeSignature;
import sun.reflect.generics.tree.SimpleClassTypeSignature;
import sun.reflect.generics.tree.TypeArgument;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.AnnotationFormatError;
import java.lang.annotation.IncompleteAnnotationException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ideal.sylph.spi.exception.StandardErrorCode.LOAD_MODULE_ERROR;
import static java.util.Objects.requireNonNull;

public class PipelinePluginLoader
{
    private static final String PREFIX = "META-INF/services/";   // copy form ServiceLoader
    private static final Logger logger = LoggerFactory.getLogger(PipelinePluginLoader.class);
    private Set<PipelinePluginManager.PipelinePluginInfo> pluginsInfo;

    public void loadPlugins()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException
    {
        File pluginsDir = new File("etl-plugins");
        if (!pluginsDir.exists() || !pluginsDir.isDirectory()) {
            throw new RuntimeException(pluginsDir + " not exists or isDirectory");
        }
        File[] pluginFiles = requireNonNull(pluginsDir.listFiles(), pluginsDir + " not exists or isDirectory");

        ImmutableSet.Builder<PipelinePluginManager.PipelinePluginInfo> builder = ImmutableSet.builder();
        for (File it : pluginFiles) {
            DirClassLoader dirClassLoader = new DirClassLoader(null, this.getClass().getClassLoader());
            dirClassLoader.addDir(it);
            Set<Class<? extends PipelinePlugin>> plugins = loadPipelinePlugins(dirClassLoader);
            Set<PipelinePluginManager.PipelinePluginInfo> tmp = plugins.stream().map(javaClass -> {
                try {
                    if (RealTimePipeline.class.isAssignableFrom(javaClass)) {
                        logger.debug("this is RealTimePipeline: {}", javaClass);
                        return getPluginInfo(it, javaClass, true, new TypeArgument[0]);
                    }
                    TypeArgument[] typeArguments = parserDriver(javaClass);
                    return getPluginInfo(it, javaClass, false, typeArguments);
                }
                catch (IncompleteAnnotationException e) {
                    throw new RuntimeException(it + " Annotation value not set, Please check scala code", e);
                }
            }).collect(Collectors.toSet());
            builder.addAll(tmp);
        }
        this.pluginsInfo = builder.build();
    }

    public Set<PipelinePluginManager.PipelinePluginInfo> getPluginsInfo()
    {
        return pluginsInfo;
    }

    private static Set<Class<? extends PipelinePlugin>> loadPipelinePlugins(ClassLoader runnerClassLoader)
            throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        final String fullName = PREFIX + PipelinePlugin.class.getName();
        final Enumeration<URL> configs = runnerClassLoader.getResources(fullName);

        Method method = ServiceLoader.class.getDeclaredMethod("parse", Class.class, URL.class);
        method.setAccessible(true);
        ImmutableSet.Builder<Class<? extends PipelinePlugin>> builder = ImmutableSet.builder();
        while (configs.hasMoreElements()) {
            URL url = configs.nextElement();
            @SuppressWarnings("unchecked") Iterator<String> iterator = (Iterator<String>) method
                    .invoke(ServiceLoader.load(PipelinePlugin.class), PipelinePlugin.class, url);
            iterator.forEachRemaining(x -> {
                Class<?> javaClass = null;
                try {
                    javaClass = Class.forName(x, false, runnerClassLoader);  // runnerClassLoader.loadClass(x)
                    if (PipelinePlugin.class.isAssignableFrom(javaClass)) {
                        logger.info("Find PipelinePlugin:{}", x);
                        builder.add((Class<? extends PipelinePlugin>) javaClass);
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

    @Beta
    private static TypeArgument[] parserDriver(Class<? extends PipelinePlugin> javaClass)
    {
        try {
            Method method = Class.class.getDeclaredMethod("getGenericInfo");
            method.setAccessible(true);
            ClassRepository classRepository = (ClassRepository) method.invoke(javaClass);
            //-----2
            Method method2 = AbstractRepository.class.getDeclaredMethod("getTree");
            method2.setAccessible(true);
            ClassSignature tree = (ClassSignature) method2.invoke(classRepository);
            ClassTypeSignature superInterface = tree.getSuperInterfaces()[0];  //type 个数  === type[]
            TypeArgument[] types = superInterface.getPath().get(0).getTypeArguments();
            List<String> typeNames = Arrays.stream(types).flatMap(x -> ((ClassTypeSignature) x).getPath().stream())
                    .map(SimpleClassTypeSignature::getName).collect(Collectors.toList());
            logger.info("--The {} is not RealTimePipeline--the Java generics is {} --", javaClass, typeNames);
            return types;
            //Type[] javaTypes = classRepository.getSuperInterfaces();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

//        Type type = javaClass.getGenericInterfaces()[0];  //获取多个范行信息 //直接获取会报出 ClassNotFoundException
//        if (type instanceof ParameterizedType) {
//            ParameterizedType parameterizedType = (ParameterizedType) type;
//            Type[] types = parameterizedType.getActualTypeArguments();
//            logger.info("--The {} is not RealTimePipeline--the Java generics is {} --", javaClass, Arrays.asList(types));
//            //return getPluginInfo(factoryClass, javaClass, PRIVATE, types);
//        }
//        else {
//            throw new RuntimeException("Unrecognized plugin:" + javaClass);
//        }
    }

    private static PipelinePluginManager.PipelinePluginInfo getPluginInfo(
            File pluginFile,
            Class<? extends PipelinePlugin> javaClass,
            boolean realTime,   //is realTime ?
            TypeArgument[] javaGenerics)
    {
        Name[] names = javaClass.getAnnotationsByType(Name.class);
        String[] nameArr = ImmutableSet.<String>builder()
                .add(javaClass.getName())
                .add(Stream.of(names).map(Name::value).toArray(String[]::new))
                .build().toArray(new String[0]);

        String isRealTime = realTime ? "RealTime" : "Not RealTime";
        logger.info("loading {} Pipeline Plugin:{} ,the name is {}", isRealTime, javaClass, nameArr);

        Description description = javaClass.getAnnotation(Description.class);
        Version version = javaClass.getAnnotation(Version.class);

        return new PipelinePluginManager.PipelinePluginInfo(
                nameArr,
                description == null ? "" : description.value(),
                version == null ? "" : version.value(),
                realTime,
                javaClass.getName(),
                javaGenerics,
                pluginFile
        );
    }
}
