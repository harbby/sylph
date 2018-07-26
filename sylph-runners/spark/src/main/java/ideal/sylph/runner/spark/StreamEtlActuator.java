package ideal.sylph.runner.spark;

import ideal.sylph.common.jvm.JVMLauncher;
import ideal.sylph.common.jvm.JVMLaunchers;
import ideal.sylph.runner.spark.etl.sparkstreaming.StreamPluginLoader;
import ideal.sylph.spi.App;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import java.io.IOException;
import java.util.Optional;

@Name("Spark_StreamETL")
@Description("spark1.x spark streaming StreamETL")
public class StreamEtlActuator
        implements JobActuator
{
    @Override
    public Job formJob(String jobId, Flow flow)
    {
        try {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setCallable(() -> {
                        SparkConf sparkConf = new SparkConf()
                                .setAppName(this.getClass().getName())
                                .setMaster("local[*]");
                        StreamingContext ssc = new StreamingContext(sparkConf, Seconds.apply(5));

                        App<StreamingContext, DStream<Row>> app = new App<StreamingContext, DStream<Row>>()
                        {
                            @Override
                            public NodeLoader<StreamingContext, DStream<Row>> getNodeLoader()
                            {
                                return new StreamPluginLoader();
                            }

                            @Override
                            public StreamingContext getContext()
                            {
                                return ssc;
                            }
                        };
                        app.build(jobId, flow);
                        return 1;
                    })
                    .build();
            launcher.startAndGet(this.getClass().getClassLoader());
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public JobContainer createJobContainer(Job job, Optional<String> jobInfo)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
