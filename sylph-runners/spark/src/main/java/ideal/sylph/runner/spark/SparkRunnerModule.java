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
package ideal.sylph.runner.spark;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import ideal.sylph.spi.exception.SylphException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.stream.Stream;

import static ideal.sylph.spi.exception.StandardErrorCode.CONFIG_ERROR;
import static java.util.Objects.requireNonNull;

public class SparkRunnerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(YarnConfiguration.class).toProvider(SparkRunnerModule::loadYarnConfiguration).in(Scopes.SINGLETON);
        binder.bind(YarnClient.class).toProvider(YarnClientProvider.class).in(Scopes.SINGLETON);
    }

    private static class YarnClientProvider
            implements Provider<YarnClient>
    {
        @Inject private YarnConfiguration yarnConfiguration;

        @Override
        public YarnClient get()
        {
            YarnClient client = YarnClient.createYarnClient();
            client.init(yarnConfiguration);
            client.start();
            return client;
        }
    }

    private static YarnConfiguration loadYarnConfiguration()
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Stream.of("yarn-site.xml", "core-site.xml", "hdfs-site.xml").forEach(file -> {
            File site = new File(requireNonNull(System.getenv("HADOOP_CONF_DIR"), "ENV HADOOP_CONF_DIR is not setting"), file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            }
            else {
                throw new SylphException(CONFIG_ERROR, site + " not exists");
            }
        });

        YarnConfiguration yarnConf = new YarnConfiguration(hadoopConf);
        //        try (PrintWriter pw = new PrintWriter(new FileWriter(yarnSite))) { //写到本地
//            yarnConf.writeXml(pw);
//        }
        return yarnConf;
    }
}
