package org.ideal.sylph.api.pipeline;

import org.ideal.sylph.api.PipelinePlugin;
import org.ideal.sylph.api.Row;

public interface RealTimeSink
        extends PipelinePlugin
{
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
