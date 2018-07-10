package ideal.sylph.main.server;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import ideal.sylph.main.controller.ControllerApp;
import ideal.sylph.main.service.LocalJobStore;
import ideal.sylph.main.service.RunnerManger;
import ideal.sylph.spi.JobStore;
import ideal.sylph.spi.RunnerContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nonnull;

import static io.airlift.configuration.ConfigBinder.configBinder;

public final class ServerMainModule
        implements Module
{
    public ServerMainModule()
    {
    }

    @Override
    public void configure(Binder binder)
    {
        //---
        configBinder(binder).bindConfig(ServerConfig.class);
        binder.bind(ControllerApp.class).in(Scopes.SINGLETON);

        binder.bind(JobStore.class).to(LocalJobStore.class).in(Scopes.SINGLETON);
        binder.bind(RunnerManger.class).in(Scopes.SINGLETON);

        binder.bind(PluginLoader.class).in(Scopes.SINGLETON);
        binder.bind(StaticJobLoader.class).in(Scopes.SINGLETON);

        binder.bind(RunnerContext.class).toProvider(RunnerContextImpl.class).in(Scopes.SINGLETON);
//        binder.bind(RunnerContext.class).toProvider(() -> new RunnerContext()
//        {
//            private final YarnConfiguration yarnConfiguration = null;
//
//            @Nonnull
//            @Override
//            public YarnConfiguration getYarnConfiguration()
//            {
//                return yarnConfiguration;
//            }
//        }).in(Scopes.SINGLETON);
    }

    @Provides
    public YarnClient createYarnClient()
    {
        YarnConfiguration yarnConfiguration = loadYarnConfiguration();
        YarnClient client = YarnClient.createYarnClient();
        client.init(yarnConfiguration);
        client.start();
        return client;
    }

    private static YarnConfiguration loadYarnConfiguration()
    {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        YarnConfiguration yarnConf = new YarnConfiguration(hadoopConf);
        //        try (PrintWriter pw = new PrintWriter(new FileWriter(yarnSite))) { //写到本地
//            yarnConf.writeXml(pw);
//        }
        return yarnConf;
    }

    private static class RunnerContextImpl
            implements Provider<RunnerContext>
    {
        private final YarnConfiguration yarnConfiguration;

        @Inject
        public RunnerContextImpl(ServerConfig serverConfig)
        {
            this.yarnConfiguration = loadYarnConfiguration();
        }

        @Override
        public RunnerContext get()
        {
            return new RunnerContext()
            {
                @Nonnull
                @Override
                public YarnConfiguration getYarnConfiguration()
                {
                    return yarnConfiguration;
                }
            };
        }
    }
}
