package ideal.sylph.runner.spark;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.stream.Stream;

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
            File site = new File(System.getenv("HADOOP_CONF_DIR"), file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            }
        });

        YarnConfiguration yarnConf = new YarnConfiguration(hadoopConf);
        //        try (PrintWriter pw = new PrintWriter(new FileWriter(yarnSite))) { //写到本地
//            yarnConf.writeXml(pw);
//        }
        return yarnConf;
    }
}
