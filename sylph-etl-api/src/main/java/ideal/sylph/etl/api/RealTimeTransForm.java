package ideal.sylph.etl.api;

import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.etl.Row;

public interface RealTimeTransForm
        extends PipelinePlugin, RealTimePipeline
{
    /**
     * line 级别的 需要注意线程安全问题
     **/
    Row[] process(Row value);

    /**
     * driver 上运行
     */
    Row.Schema getRowSchema();
}
