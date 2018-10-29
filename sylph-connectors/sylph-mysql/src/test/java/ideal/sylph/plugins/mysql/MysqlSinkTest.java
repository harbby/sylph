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
package ideal.sylph.plugins.mysql;

import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;
import org.junit.Assert;
import org.junit.Test;
import sun.reflect.ReflectionFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

public class MysqlSinkTest
{
    @Test
    public void parserPluginTest()
            throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException
    {
        Constructor constructor = MysqlSink.class.getConstructors()[0];

        for (Class<?> type : constructor.getParameterTypes()) {
            if (PluginConfig.class.isAssignableFrom(type)) {
                try {
                    type.getDeclaredConstructor();
                    Assert.assertTrue(true);
                }
                catch (NoSuchMethodException e) {
                    Assert.fail();
                }

                PluginConfig pluginConfig = (PluginConfig) getInstance(type);
                for (Field field : type.getDeclaredFields()) {
                    Name name = field.getAnnotation(Name.class);
                    if (name != null) {
                        field.setAccessible(true);
                        Assert.assertNotNull(name);
                        field.set(pluginConfig, "@Name[" + name.value() + "]");
                    }
                }
                Assert.assertNotNull(pluginConfig);
                Assert.assertNotNull(pluginConfig.toString());
                System.out.println(type + " class -> " + pluginConfig);
                Object mysqlSink = constructor.newInstance(pluginConfig);
                Assert.assertTrue(mysqlSink instanceof MysqlSink);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getInstance(Class<T> type)
            throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException
    {
        Constructor superCons = Object.class.getConstructor();  //获取Object的构造器
        ReflectionFactory reflFactory = ReflectionFactory.getReflectionFactory();
        Constructor<T> c = (Constructor<T>) reflFactory.newConstructorForSerialization(type, superCons);
        return c.newInstance();
    }
}