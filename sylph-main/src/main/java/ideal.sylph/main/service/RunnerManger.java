package ideal.sylph.main.service;

import com.google.inject.Inject;
import ideal.sylph.spi.Job;
import ideal.sylph.spi.JobActuator;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.annotation.Name;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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

    public synchronized void createRunner(Runner runner)
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
     * 运行任务
     */
    public void runJob(@Nonnull Job job)
    {
        String jobType = requireNonNull(job.getJobActuatorName(), "job Actuator Name is null " + job.getJobId());
        var jobActuator = jobActuatorMap.get(jobType);
        checkArgument(jobActuator != null, jobType + " not exists");
        jobActuator.execJob(job);
    }

    public Job formJobWithDir(File jobDir, Map<String, String> jobProps)
    {
        String jobType = requireNonNull(jobProps.get("type"), "jobProps arg type is null");
        var jobActuator = jobActuatorMap.get(jobType);
        checkArgument(jobActuator != null, "job [" + jobDir + "] loading error! JobActuator:[" + jobType + "] not exists,only " + jobActuatorMap.keySet());
        return jobActuator.formJob(jobDir);
    }
}
