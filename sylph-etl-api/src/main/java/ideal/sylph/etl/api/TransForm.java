package ideal.sylph.etl.api;

import ideal.sylph.etl.PipelinePlugin;

import java.util.Map;

/**
 * Created by ideal on 17-5-8. 转换
 */
public interface TransForm<T>
        extends PipelinePlugin
{
    /**
     * 初始化(driver阶段执行) 需要注意序列化问题
     */
    default void driverInit(Map<String, Object> optionMap) {}

    T transform(final T stream);
}
