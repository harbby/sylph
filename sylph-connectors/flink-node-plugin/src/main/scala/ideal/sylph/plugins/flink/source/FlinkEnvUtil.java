package ideal.sylph.plugins.flink.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public final class FlinkEnvUtil
{
    private FlinkEnvUtil() {}

    public static StreamExecutionEnvironment getFlinkEnv(StreamTableEnvironment tableEnv)
    {
        return tableEnv.execEnv();
    }
}
