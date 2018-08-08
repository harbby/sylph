package ideal.sylph.main.service;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.sylph.annotation.Name;
import ideal.sylph.common.base.LazyReference;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.RunnerFactory;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.classloader.ThreadContextClassLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.AnnotationFormatError;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static ideal.sylph.spi.exception.StandardErrorCode.LOAD_MODULE_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * RunnerManager
 */
public class RunnerManager
{
    private static final Logger logger = LoggerFactory.getLogger(RunnerManager.class);
    private static final String PREFIX = "META-INF/services/";   // copy form ServiceLoader
    private final Map<String, JobActuator> jobActuatorMap = new HashMap<>();

    @Inject
    public RunnerManager() {}

    public void createRunner(RunnerFactory factory)
    {
        logger.info("===== Runner: {} starts loading {} =====", factory.getClass().getName(), PipelinePlugin.class.getName());
        final Set<Class<?>> plugins;
        try {
            plugins = loadPipelinePlugins(factory.getClass().getClassLoader());
        }
        catch (Exception e) {
            throw new RuntimeException(e); //test
        }
        LazyReference<PipelinePluginManager> lazyGetter = LazyReference.goLazy(() -> new PipelinePluginManagerImpl(plugins, factory.getClass()));
        RunnerContext runnerContext = lazyGetter::get;

        final Runner runner = factory.create(runnerContext);
        runner.getJobActuators().forEach(jobActuatorHandle -> {
            JobActuator jobActuator = new JobActuatorImpl(jobActuatorHandle);
            for (String name : jobActuator.getInfo().getName()) {
                if (jobActuatorMap.containsKey(name)) {
                    throw new IllegalArgumentException(String.format("Multiple entries with same key: %s=%s and %s=%s", name, jobActuatorMap.get(name), name, jobActuator));
                }
                else {
                    jobActuatorMap.put(name, jobActuator);
                }
            }
        });
    }

    /**
     * 创建job 运行时
     */
    JobContainer createJobContainer(@Nonnull Job job, String jobInfo)
    {
        String jobType = requireNonNull(job.getActuatorName(), "job Actuator Name is null " + job.getId());
        JobActuator jobActuator = jobActuatorMap.get(jobType);
        checkArgument(jobActuator != null, jobType + " not exists");
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(jobActuator.getHandleClassLoader())) {
            return jobActuator.getHandle().createJobContainer(job, jobInfo);
        }
    }

    Job formJobWithDir(File jobDir, Map<String, String> jobProps)
    {
        String jobType = requireNonNull(jobProps.get("type"), "jobProps arg type is null");
        try {
            byte[] flowBytes = Files.readAllBytes(Paths.get(new File(jobDir, "job.yaml").toURI()));
            return formJobWithFlow(jobDir.getName(), flowBytes, jobType);
        }
        catch (IOException e) {
            throw new SylphException(JOB_BUILD_ERROR, "loadding job " + jobDir + " job.yaml fail", e);
        }
    }

    public Job formJobWithFlow(String jobId, byte[] flowBytes, String actuatorName)
            throws IOException
    {
        requireNonNull(actuatorName, "job actuatorName is null");
        JobActuator jobActuator = jobActuatorMap.get(actuatorName);
        checkArgument(jobActuator != null, "job [" + jobId + "] loading error! JobActuator:[" + actuatorName + "] not exists,only " + jobActuatorMap.keySet());

        File jobDir = new File("jobs/" + jobId);
        DirClassLoader jobClassLoader = new DirClassLoader(null, jobActuator.getHandleClassLoader());
        jobClassLoader.addDir(jobDir);
        Flow flow = jobActuator.getHandle().formFlow(flowBytes);
        JobHandle jobHandle = jobActuator.getHandle().formJob(jobId, flow, jobClassLoader);
        return new Job()
        {
            @NotNull
            @Override
            public String getId()
            {
                return jobId;
            }

            @Override
            public File getWorkDir()
            {
                return jobDir;
            }

            @Override
            public URLClassLoader getJobClassLoader()
            {
                return jobClassLoader;
            }

            @NotNull
            @Override
            public String getActuatorName()
            {
                return actuatorName;
            }

            @NotNull
            @Override
            public JobHandle getJobHandle()
            {
                return jobHandle;
            }

            @NotNull
            @Override
            public Flow getFlow()
            {
                return flow;
            }
        };
    }

    public Collection<JobActuator.ActuatorInfo> getAllActuatorsInfo()
    {
        return jobActuatorMap.values()
                .stream()
                .distinct().map(JobActuator::getInfo)
                .collect(Collectors.toList());
    }

    private static Set<Class<?>> loadPipelinePlugins(ClassLoader runnerClassLoader)
            throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        final String fullName = PREFIX + PipelinePlugin.class.getName();
        final Enumeration<URL> configs = runnerClassLoader.getResources(fullName);

        Method method = ServiceLoader.class.getDeclaredMethod("parse", Class.class, URL.class);
        method.setAccessible(true);
        ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();
        while (configs.hasMoreElements()) {
            URL url = configs.nextElement();
            @SuppressWarnings("unchecked") Iterator<String> iterator = (Iterator<String>) method
                    .invoke(ServiceLoader.load(Object.class), PipelinePlugin.class, url);
            iterator.forEachRemaining(x -> {
                Class<?> javaClass = null;
                try {
                    javaClass = Class.forName(x, false, runnerClassLoader);  // runnerClassLoader.loadClass(x)
                    if (PipelinePlugin.class.isAssignableFrom(javaClass)) {
                        Name[] names = javaClass.getAnnotationsByType(Name.class);
                        logger.info("Find PipelinePlugin:{} name is {}", x, Stream.of(names).map(Name::value).collect(Collectors.toSet()));

                        builder.add(javaClass);
                        //parserDriver(javaClass);
                    }
                    else {
                        logger.warn("UNKNOWN java class " + javaClass);
                    }
                }
                catch (AnnotationFormatError e) {
                    String errorMsg = "this scala class " + javaClass + " not getAnnotationsByType please see: https://issues.scala-lang.org/browse/SI-9529";
                    throw new SylphException(LOAD_MODULE_ERROR, errorMsg, e);
                }
                catch (Exception e) {
                    throw new SylphException(LOAD_MODULE_ERROR, e);
                }
            });
        }
        return builder.build();
    }
}
