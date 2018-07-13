package ideal.sylph.runner.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

/**
 * App对象都会在编译器进程中被被编译
 */
public interface FlinkApp
        extends Serializable
{
    /**
     * 在这个方法里描述app的 逻辑dag
     */
    void build(StreamExecutionEnvironment execEnv)
            throws Exception;

    /**
     * 获取job的类加载器
     */
    default ClassLoader getClassLoader()
    {
        return this.getClass().getClassLoader();
    }
}
