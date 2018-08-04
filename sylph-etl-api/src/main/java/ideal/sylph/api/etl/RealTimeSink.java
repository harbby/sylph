package ideal.sylph.api.etl;

import ideal.sylph.api.PipelinePlugin;
import ideal.sylph.api.Row;

import java.util.Map;

public interface RealTimeSink
        extends PipelinePlugin
{
    /**
     * 初始化(driver阶段执行) 需要注意序列化问题
     */
    default void driverInit(Map<String, Object> optionMap) {}

    /**
     * partition级别的初始化
     **/
    boolean open(long partitionId, long version);

    /**
     * line 级别的 需要注意线程安全问题
     **/
    void process(Row value);

    /**
     * partition级别的资源释放
     **/
    void close(Throwable errorOrNull);
}
