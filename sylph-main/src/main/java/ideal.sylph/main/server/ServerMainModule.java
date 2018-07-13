package ideal.sylph.main.server;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import ideal.sylph.controller.ControllerApp;
import ideal.sylph.controller.ServerConfig;
import ideal.sylph.main.service.LocalJobStore;
import ideal.sylph.main.service.RunnerManger;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.job.JobStore;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.validation.constraints.NotNull;

import static io.airlift.configuration.ConfigBinder.configBinder;

public final class ServerMainModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        //--- controller ---
        configBinder(binder).bindConfig(ServerConfig.class);
        binder.bind(ControllerApp.class).in(Scopes.SINGLETON);

        binder.bind(JobStore.class).to(LocalJobStore.class).in(Scopes.SINGLETON);

        //  --- Binding parameter
        //  binder.bindConstant().annotatedWith(Names.named("redis.hosts")).to("localhost:6379");
        //  Names.bindProperties(binder, new Properties());

        binder.bind(RunnerManger.class).in(Scopes.SINGLETON);
        binder.bind(PluginLoader.class).in(Scopes.SINGLETON);

        binder.bind(SylphContext.class).toProvider(()->new SylphContext(){}).in(Scopes.SINGLETON);

        binder.bind(RunnerContext.class).toProvider(RunnerContextImpl.class).in(Scopes.SINGLETON);
    }

    @Provides   //(隐式绑定 区别与上面配置的显式绑定)
    public static YarnClient createYarnClient()
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
                @NotNull
                @Override
                public YarnConfiguration getYarnConfiguration()
                {
                    return yarnConfiguration;
                }
            };
        }
    }
}
