package ideal.sylph.runner.spark;

import com.google.inject.Inject;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.common.jvm.JVMException;
import ideal.sylph.common.jvm.JVMLauncher;
import ideal.sylph.common.jvm.JVMLaunchers;
import ideal.sylph.runner.spark.etl.sparkstreaming.StreamPluginLoader;
import ideal.sylph.spi.App;
import ideal.sylph.spi.GraphApp;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.PipelinePluginManager;
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
    @Inject private PipelinePluginManager pluginManager;

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow flow, URLClassLoader jobClassLoader)
    {
        final Supplier<App<StreamingContext>> appGetter = (Supplier<App<StreamingContext>> & Serializable) () -> new GraphApp<StreamingContext, DStream<Row>>()
        {
            private final StreamingContext spark = new StreamingContext(new SparkConf(), Seconds.apply(5));

            @Override
            public NodeLoader<StreamingContext, DStream<Row>> getNodeLoader()
            {
                return new StreamPluginLoader(pluginManager);
            }

            @Override
            public StreamingContext getContext()
            {
                return spark;
            }

            @Override
            public void build()
                    throws Exception
            {
                this.buildGraph(jobId, flow).run();
            }
        };

        try {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setCallable(() -> {
                        appGetter.get().build();
                        return 1;
                    })
                    .addUserURLClassLoader(jobClassLoader)
                    .build();
            launcher.startAndGet(jobClassLoader);
            return new SparkJobHandle<>(appGetter);
        }
        catch (IOException | ClassNotFoundException | JVMException e) {
            throw new SylphException(JOB_BUILD_ERROR, "JOB_BUILD_ERROR", e);
        }
    }
}
