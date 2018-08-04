package ideal.sylph.main.service;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobActuatorHandle;

import java.net.URLClassLoader;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JobActuatorImpl
        implements JobActuator
{
    private final long startTime = System.currentTimeMillis();
    private final JobActuator.ActuatorInfo info;
    private final JobActuatorHandle jobActuatorHandle;

    private final String[] name;
    private final String description;

    JobActuatorImpl(JobActuatorHandle jobActuatorHandle)
    {
        this.jobActuatorHandle = requireNonNull(jobActuatorHandle, "jobActuatorHandle is null");
        this.name = buildName(jobActuatorHandle);
        this.description = buildDescription(jobActuatorHandle);
        this.info = new JobActuator.ActuatorInfo()
        {
            @Override
            public String[] getName()
            {
                return name;
            }

            @Override
            public String getDescription()
            {
                return description;
            }

            @Override
            public long getCreateTime()
            {
                return startTime;
            }

            @Override
            public String getVersion()
            {
                return "none";
            }
        };
    }

    @Override
    public JobActuatorHandle getHandle()
    {
        return jobActuatorHandle;
    }

    @Override
    public ActuatorInfo getInfo()
    {
        return info;
    }

    @Override
    public URLClassLoader getHandleClassLoader()
    {
        return (URLClassLoader) jobActuatorHandle.getClass().getClassLoader();
    }

    private String[] buildName(JobActuatorHandle jobActuator)
    {
        String errorMessage = jobActuator.getClass().getName() + " Missing @Name annotation";
        //Name actuatorName = jobActuator.getClass().getAnnotation(Name.class);
        Name[] actuatorNames = jobActuator.getClass().getAnnotationsByType(Name.class);  //多重注解
        String[] names = Stream.of(requireNonNull(actuatorNames, errorMessage))
                .map(Name::value).distinct()
                .toArray(String[]::new);
        checkState(names.length > 0, errorMessage);
        return names;
    }

    private String buildDescription(JobActuatorHandle jobActuator)
    {
        String errorMessage = jobActuator.getClass().getName() + " Missing @Name annotation";
        Description description = jobActuator.getClass().getAnnotation(Description.class);
        return requireNonNull(description, errorMessage).value();
    }
}
