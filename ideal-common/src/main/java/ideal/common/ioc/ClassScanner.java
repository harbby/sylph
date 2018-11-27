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
package ideal.common.ioc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static ideal.common.base.Files.listFiles;

public class ClassScanner
{
    private ClassScanner() {}

    private static final Logger logger = LoggerFactory.getLogger(ClassScanner.class);

    public static Set<Class<?>> getClasses(String basePackage)
            throws IOException
    {
        //Package slf4j = Package.getPackage("org.slf4j");
        ClassLoader classLoader = sun.misc.VM.latestUserDefinedLoader();

        return getClasses(basePackage, classLoader, logger::warn);
    }

    public static Set<Class<?>> getClasses(String basePackage, ClassLoader classLoader, BiConsumer<String, Throwable> handler)
            throws IOException
    {
        Set<String> classStrings = scanClasses(basePackage, classLoader);

        Set<Class<?>> classes = new HashSet<>();
        for (String it : classStrings) {
            String classString = it.substring(0, it.length() - 6).replace("/", ".");

            try {
                Class<?> driver = Class.forName(classString, false, classLoader);  //classLoader.loadClass(classString)
                classes.add(driver);  //
            }
            catch (Throwable e) {
                handler.accept(classString, e);
            }
        }
        return classes;
    }

    public static Set<String> scanClasses(String basePackage, ClassLoader classLoader)
            throws IOException
    {
        String packagePath = basePackage.replace('.', '/');

        Set<String> classStrings = new HashSet<>();
        Enumeration<URL> resources = classLoader.getResources(packagePath);
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            String protocol = url.getProtocol();
            if ("file".equals(protocol)) {
                classStrings.addAll(scanFileClass(packagePath, url, true));
            }
            else if ("jar".equals(protocol)) {
                classStrings.addAll(scanJarClass(packagePath, url));
            }
        }

        return classStrings;
    }

    private static Set<String> scanJarClass(String packagePath, URL url)
            throws IOException
    {
        JarFile jarFile = ((JarURLConnection) url.openConnection()).getJarFile();

        Set<String> classSet = new HashSet<>();
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String name = entry.getName();
            if (name.charAt(0) == '/') {
                name = name.substring(1);
            }
            if (!name.startsWith(packagePath)) {
                continue;
            }

            if (name.endsWith(".class") && !entry.isDirectory()) {
                classSet.add(name);
            }
        }
        return classSet;
    }

    private static Set<String> scanFileClass(String packagePath, URL url, boolean recursive)
    {
        List<File> files = listFiles(new File(url.getPath()), recursive);
        return files.stream().map(file -> {
            String path = file.getPath();
            int start = path.indexOf(packagePath);
            return path.substring(start);
        }).collect(Collectors.toSet());
    }
}
