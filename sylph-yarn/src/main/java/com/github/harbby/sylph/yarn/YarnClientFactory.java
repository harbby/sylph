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
package com.github.harbby.sylph.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class YarnClientFactory
{
    private YarnClientFactory() {}

    public static YarnClient createYarnClient()
            throws IOException
    {
        Configuration yarnConfiguration = loadYarnConfiguration();
        YarnClient client = YarnClient.createYarnClient();
        client.init(yarnConfiguration);
        client.start();
        return client;
    }

    public static Configuration loadYarnConfiguration()
            throws FileNotFoundException
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (hadoopConfDir == null) {
            throw new UnsupportedOperationException("ENV HADOOP_CONF_DIR is not setting");
        }
        for (String file : new String[] {"yarn-site.xml", "core-site.xml", "hdfs-site.xml"}) {
            File site = new File(hadoopConfDir, file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            }
            else {
                throw new FileNotFoundException("ENV HADOOP_CONF_DIR error, NOT Found HADOOP file: " + site);
            }
        }
        return hadoopConf;
    }
}
