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
package ideal.sylph.main.util;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

public final class PropertiesUtil
{
    private PropertiesUtil() {}

    public static Properties loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return properties;
    }

    public static Map<String, String> loadPropertiesAsMap(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }

    public static ImmutableMap<String, String> fromProperties(Properties properties)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            builder.put(key, properties.getProperty(key));
        }

        return builder.build();
    }
}
