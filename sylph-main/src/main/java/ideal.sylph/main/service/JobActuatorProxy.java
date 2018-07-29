package ideal.sylph.main.service;

import ideal.sylph.common.proxy.DynamicProxy;
import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.job.JobActuator;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JobActuatorProxy
        extends DynamicProxy
{
    private final long startTime = System.currentTimeMillis();
    private final JobActuator.ActuatorInfo info;

    private final String[] name;

    private final String description;

    JobActuatorProxy(JobActuator jobActuator)
    {
        super(jobActuator);
        this.name = buildName(jobActuator);
        this.description = buildDescription(jobActuator);
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
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        if ("getInfo".equals(method.getName())) {
            return info;
        }
        return super.invoke(proxy, method, args);
    }

    private String[] buildName(JobActuator jobActuator)
    {
        String errorMessage = jobActuator.getClass().getName() + " Missing @Name annotation";
        Name actuatorName = jobActuator.getClass().getAnnotation(Name.class);
        String[] names = Stream.of(requireNonNull(actuatorName, errorMessage).value()).distinct().toArray(String[]::new);
        checkState(names.length > 0, errorMessage);
        return names;
    }

    private String buildDescription(JobActuator jobActuator)
    {
        String errorMessage = jobActuator.getClass().getName() + " Missing @Name annotation";
        Description description = jobActuator.getClass().getAnnotation(Description.class);
        return requireNonNull(description, errorMessage).value();
    }
}
