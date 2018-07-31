package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.net.URLClassLoader;

public interface Job
{
    @NotNull
    public String getId();

    default String getDescription()
    {
        return "none";
    }

    File getWorkDir();

    URLClassLoader getJobClassLoader();

    @NotNull
    String getActuatorName();

    @NotNull
    JobHandle getJobHandle();

    @NotNull
    Flow getFlow();

    public enum Status
    {
        RUNNING(0),   //运行中
        STARTING(1),    // 启动中
        STOP(2),           // 停止运行
        START_ERROR(3);           // 启动失败

        private final int status;

        Status(int code)
        {
            this.status = code;
        }
    }
}
