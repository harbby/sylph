package ideal.sylph.etl.api;

import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.etl.Row;

public interface RealTimeSink
        extends PipelinePlugin, RealTimePipeline
{
    /**
     * line 级别的 需要注意线程安全问题
     **/
    void process(Row value);
}
