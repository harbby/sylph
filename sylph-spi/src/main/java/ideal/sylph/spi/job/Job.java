package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

public interface Job
{
    @NotNull
    public String getId();

    default String getDescription()
    {
        return "none";
    }

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
