package org.ideal.sylph.api.pipeline;

import java.io.Serializable;
import java.util.Map;

public interface Source<T, R>
        extends Serializable
{
    /**
     * 初始化(driver阶段执行) 这里需要传入sess
     **/
    void driverInit(T sess, Map<String, Object> optionMap);

    R getSource();
}
