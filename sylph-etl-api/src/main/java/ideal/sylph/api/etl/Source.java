package ideal.sylph.api.etl;

import ideal.sylph.api.PipelinePlugin;

import java.util.Map;

public interface Source<T, R>
        extends PipelinePlugin
{
    /**
     * 初始化(driver阶段执行) 这里需要传入sess
     **/
    void driverInit(T sess, Map<String, Object> optionMap);

    R getSource();
}
