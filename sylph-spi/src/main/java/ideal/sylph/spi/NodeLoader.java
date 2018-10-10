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
package ideal.sylph.spi;

import com.google.common.collect.ImmutableList;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewConstructor;
import javassist.LoaderClassPath;
import javassist.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.ReflectionFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkState;

public interface NodeLoader<R>
{
    Logger logger = LoggerFactory.getLogger(NodeLoader.class);

    public UnaryOperator<R> loadSource(String driverStr, final Map<String, Object> pluginConfig);

    public UnaryOperator<R> loadTransform(String driverStr, final Map<String, Object> pluginConfig);

    public UnaryOperator<R> loadSink(String driverStr, final Map<String, Object> pluginConfig);

    /**
     * This method will generate the instance object by injecting the PipeLine interface.
     */
    default <T> T getInstance(Class<T> driver, Map<String, Object> config)
            throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException, CannotCompileException, NoSuchFieldException, NotFoundException
    {
        @SuppressWarnings("unchecked")
        Constructor<T>[] constructors = (Constructor<T>[]) driver.getConstructors();
        checkState(constructors.length == 1, String.format("%s has multiple public constructors, please ensure that there is only one", driver));
        final Constructor<T> constructor = constructors[0];

        if (constructor.getParameters().length == 0) {
            logger.info("plugin class [{}] not find 'no parameter' Constructor, using class.newInstance()", driver);
            return driver.newInstance();
        }

        constructor.setAccessible(true);
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        for (Class<?> type : constructor.getParameterTypes()) {
            if (PluginConfig.class.isAssignableFrom(type)) { //config injection
                PluginConfig pluginConfig = getPipeConfigInstance(type.asSubclass(PluginConfig.class), this.getClass().getClassLoader());
                //--- inject map config
                injectConfig(pluginConfig, config);
                builder.add(pluginConfig);
            }
            else {
                Object value = getBinds().get(type);
                if (value == null) {
                    throw new IllegalArgumentException(String.format("Cannot find instance of parameter [%s], unable to inject", type));
                }
                builder.add(value);
            }
        }
        return constructor.newInstance(builder.build().toArray());
    }

    static PluginConfig getPipeConfigInstance(Class<? extends PluginConfig> type, ClassLoader classLoader)
            throws IllegalAccessException, InvocationTargetException, InstantiationException, NotFoundException, CannotCompileException, NoSuchMethodException, NoSuchFieldException
    {
        if (type.isInterface() || Modifier.isAbstract(type.getModifiers())) {
            throw new IllegalArgumentException(type + " is Interface or Abstract, unable to inject");
        }

        //Ignore the constructor in the configuration class
        try {
            Constructor pluginConfigConstructor = type.getDeclaredConstructor();
            logger.info("[PluginConfig] find 'no parameter' constructor with [{}]", type);
            pluginConfigConstructor.setAccessible(true);
            PluginConfig pluginConfig = (PluginConfig) pluginConfigConstructor.newInstance();
            return pluginConfig;
        }
        catch (NoSuchMethodException e) {
            logger.warn("[PluginConfig] not find 'no parameter' constructor, use javassist inject with [{}]", type);
            ClassPool classPool = new ClassPool();
            classPool.appendClassPath(new LoaderClassPath(classLoader));

            CtClass ctClass = classPool.get(type.getName());

            ctClass.addConstructor(CtNewConstructor.defaultConstructor(ctClass));  //add 'no parameter' constructor
            ctClass.setName("javassist." + type.getName());
            Class<?> proxyClass = ctClass.toClass();
            PluginConfig proxy = (PluginConfig) proxyClass.newInstance();

            // copy proxyConfig field value to pluginConfig ...
            Constructor superCons = Object.class.getConstructor();
            ReflectionFactory reflFactory = ReflectionFactory.getReflectionFactory();
            Constructor c = reflFactory.newConstructorForSerialization(type, superCons);
            // or use unsafe, demo: PluginConfig pluginConfig = (PluginConfig) unsafe.allocateInstance(type)
            PluginConfig pluginConfig = (PluginConfig) c.newInstance();
            for (Field field : type.getDeclaredFields()) {
                field.setAccessible(true);
                Field proxyField = proxyClass.getDeclaredField(field.getName());
                proxyField.setAccessible(true);

                if (Modifier.isStatic(field.getModifiers())) {
                    //&& Modifier.isFinal(field.getModifiers())  Can not set static final
                    //field.set(null, proxyField.get(null));
                }
                else {
                    field.set(pluginConfig, proxyField.get(proxy));
                }
            }
            logger.info("copied  proxyClass to {}, the proxyObj is {}", type, pluginConfig);
            return pluginConfig;
        }
    }

    static void injectConfig(PluginConfig pluginConfig, Map config)
            throws IllegalAccessException
    {
        Class<?> typeClass = pluginConfig.getClass();
        for (Field field : typeClass.getDeclaredFields()) {
            Name name = field.getAnnotation(Name.class);
            if (name != null) {
                field.setAccessible(true);
                Object value = config.get(name.value());
                if (value != null) {
                    field.set(pluginConfig, value);
                }
                else if (field.get(pluginConfig) == null) {
                    // Unable to inject via config, and there is no default value
                    String errorMsg = String.format("[PluginConfig] %s field %s[%s] unable to inject ,and there is no default value, config only %s", typeClass, field.getName(), name.value(), config);
                    throw new IllegalArgumentException(errorMsg);
                }
            }
        }
        logger.info("inject pluginConfig Class [{}], outObj is {}", typeClass, pluginConfig);
    }

    public Binds getBinds();
}
