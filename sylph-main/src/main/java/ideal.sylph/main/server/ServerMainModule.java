package ideal.sylph.main.server;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import ideal.sylph.controller.ControllerApp;
import ideal.sylph.controller.ServerConfig;
import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.LocalJobStore;
import ideal.sylph.main.service.MetadataManager;
import ideal.sylph.main.service.PipelinePluginLoader;
import ideal.sylph.main.service.RunnerManager;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.job.JobStore;

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

        configBinder(binder).bindConfig(ServerMainConfig.class);
        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(JobStore.class).to(LocalJobStore.class).in(Scopes.SINGLETON);

        //  --- Binding parameter
        //  binder.bindConstant().annotatedWith(Names.named("redis.hosts")).to("localhost:6379");
        //  Names.bindProperties(binder, new Properties());

        binder.bind(PipelinePluginLoader.class).in(Scopes.SINGLETON);
        binder.bind(RunnerManager.class).in(Scopes.SINGLETON);
        binder.bind(RunnerLoader.class).in(Scopes.SINGLETON);
        binder.bind(JobManager.class).in(Scopes.SINGLETON);

        binder.bind(SylphContext.class).toProvider(SylphContextProvider.class).in(Scopes.SINGLETON);
    }

    private static class SylphContextProvider
            implements Provider<SylphContext>
    {
        @Inject private JobManager jobManager;
        @Inject private RunnerManager runnerManger;

        @Override
        public SylphContext get()
        {
            return new SylphContextImpl(jobManager, runnerManger);
        }
    }
}
