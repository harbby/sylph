package ideal.sylph.etl.api;

import java.util.Map;

public interface RealTimePipeline
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
     * partition级别的资源释放
     **/
    void close(Throwable errorOrNull);
}
