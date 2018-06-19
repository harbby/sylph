package org.ideal.sylph.api;

import java.io.Serializable;
import java.util.Map;

public interface PipelinePlugin
        extends Serializable
{
    /**
     * 初始化(driver阶段执行) 需要注意序列化问题
     */
    default void driverInit(Map<String, Object> optionMap) {}
}
