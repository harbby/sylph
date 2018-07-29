package ideal.sylph.runner.spark;

import ideal.sylph.common.graph.Graph;
import ideal.sylph.common.jvm.JVMLauncher;
import ideal.sylph.common.jvm.JVMLaunchers;
import ideal.sylph.common.jvm.JVMRunningException;
import ideal.sylph.runner.spark.etl.sparkstreaming.StreamPluginLoader;
import ideal.sylph.spi.App;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobHandle;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLClassLoader;
import java.util.function.Supplier;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

@Name("Spark_StreamETL")
@Description("spark1.x spark streaming StreamETL")
public class StreamEtlActuator
        extends Stream2EtlActuator
{
    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow flow)
    {
        final Supplier<App<StreamingContext, DStream<Row>>> appGetter = (Supplier<App<StreamingContext, DStream<Row>>> & Serializable) () -> new App<StreamingContext, DStream<Row>>()
        {
            private final StreamingContext spark = new StreamingContext(new SparkConf(), Seconds.apply(5));

            @Override
            public NodeLoader<StreamingContext, DStream<Row>> getNodeLoader()
            {
                return new StreamPluginLoader();
            }

            @Override
            public StreamingContext getContext()
            {
                return spark;
            }

            @Override
            public Graph<DStream<Row>> build()
            {
                return App.super.build(jobId, flow);
            }
        };

        try {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setCallable(() -> {
                        App<StreamingContext, DStream<Row>> app = appGetter.get();
                        app.build().run();
                        return 1;
                    })
                    .addUserURLClassLoader((URLClassLoader) this.getClass().getClassLoader())
                    .build();
            launcher.startAndGet(this.getClass().getClassLoader());
            return new SparkJobHandle<>(appGetter);
        }
        catch (IOException | ClassNotFoundException | JVMRunningException e) {
            throw new SylphException(JOB_BUILD_ERROR, "JOB_BUILD_ERROR", e);
        }
    }
}
