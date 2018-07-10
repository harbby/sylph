package ideal.sylph.spi;

import javax.annotation.Nonnull;

public interface Job
{
    @Nonnull
    public String getJobId();

    String getDescription();

    @Nonnull
    String getJobActuatorName();

    /**
     * get Is it online?
     */
    boolean getIsOnline();

    public enum State
    {
        RUNNING(0),   //运行中
        SUBMITTING(1),    // 启动中
        STOP(2),           // 停止运行
        START_ERROR(3);           // 启动失败

        private final int state;

        State(int code)
        {
            this.state = code;
        }

        public boolean equals(State inState)
        {
            return inState.state == this.state;
        }
    }
}
