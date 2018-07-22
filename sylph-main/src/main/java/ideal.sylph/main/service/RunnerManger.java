package ideal.sylph.main.service;

import com.google.inject.Inject;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.classloader.ThreadContextClassLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * RunnerManger
 */
public class RunnerManger
{
    private final Map<String, JobActuator> jobActuatorMap = new HashMap<>();
    private final RunnerContext runnerContext;

    @Inject
    public RunnerManger(
            RunnerContext runnerContext
    )
    {
        this.runnerContext = requireNonNull(runnerContext, "runnerContext is null");
    }

    public void createRunner(Runner runner)
    {
        runner.create(runnerContext).forEach(jobActuator -> {
            String errorMessage = jobActuator.getClass().getName() + " Missing @Name annotation";
            Name actuatorName = jobActuator.getClass().getAnnotation(Name.class);
            String[] names = Stream.of(requireNonNull(actuatorName, errorMessage).value()).distinct().toArray(String[]::new);
            checkState(names.length > 0, errorMessage);
            for (String name : names) {
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
    public JobContainer createJobContainer(@Nonnull Job job, Optional<String> jobInfo)
    {
        String jobType = requireNonNull(job.getActuatorName(), "job Actuator Name is null " + job.getId());
        JobActuator jobActuator = jobActuatorMap.get(jobType);
        checkArgument(jobActuator != null, jobType + " not exists");
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(jobActuator.getClass().getClassLoader())) {
            return jobActuator.createJobContainer(job, jobInfo);
        }
    }

    public Job formJobWithDir(File jobDir, Map<String, String> jobProps)
    {
        String jobType = requireNonNull(jobProps.get("type"), "jobProps arg type is null");
        try {
            Flow flow = YamlFlow.load(new File(jobDir, "job.yaml"));
            return formJobWithFlow(jobDir.getName(), flow, jobType);
        }
        catch (IOException e) {
            throw new SylphException(JOB_BUILD_ERROR, "loadding job " + jobDir + " job.yaml fail", e);
        }
    }

    public Job formJobWithFlow(String jobId, Flow flow, String actuatorName)
    {
        requireNonNull(actuatorName, "job actuatorName is null");
        JobActuator jobActuator = jobActuatorMap.get(actuatorName);
        checkArgument(jobActuator != null, "job [" + jobId + "] loading error! JobActuator:[" + actuatorName + "] not exists,only " + jobActuatorMap.keySet());
        return jobActuator.formJob(jobId, flow);
    }
}
