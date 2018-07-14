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
import ideal.sylph.main.service.RunnerManger;
import ideal.sylph.main.service.YamlFlow;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobStore;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

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
        binder.bind(JobManager.class).in(Scopes.SINGLETON);

        binder.bind(SylphContext.class).toProvider(SylphContextImpl.class).in(Scopes.SINGLETON);
        binder.bind(RunnerContext.class).toProvider(RunnerContextImpl.class).in(Scopes.SINGLETON);
    }

    private static class SylphContextImpl
            implements Provider<SylphContext>
    {
        @Inject
        private JobManager jobManager;

        @Inject
        private RunnerManger runnerManger;

        @Override
        public SylphContext get()
        {
            return new SylphContext()
            {
                @Override
                public void saveJob(String jobId, String flow, String actuatorName)
                        throws Exception
                {
                    runnerManger.formJobWithFlow(jobId, YamlFlow.load(flow), actuatorName);
                }

                @Override
                public void stopJob(String jobId)
                {
                    jobManager.stopJob(jobId);
                }

                @Override
                public void startJob(String jobId)
                {
                    jobManager.startJob(jobId);
                }

                @Override
                public void deleteJob(String jobId)
                {
                    jobManager.removeJob(jobId);
                }

                @Override
                public Collection<Job> getAllJobs()
                {
                    return jobManager.listJobs();
                }

                @Override
                public Optional<Job> getJob(String jobId)
                {
                    return jobManager.getJob(jobId);
                }

                @Override
                public Optional<JobContainer> getJobContainer(String jobId)
                {
                    return jobManager.getJobContainer(jobId);
                }
            };
        }
    }

    private static YarnConfiguration loadYarnConfiguration()
    {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Stream.of("yarn-site.xml", "core-site.xml").forEach(file -> {
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
