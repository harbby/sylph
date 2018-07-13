package ideal.sylph.runner.flink.runtime;

import com.google.inject.Inject;
import ideal.sylph.runner.flink.FlinkApp;
import ideal.sylph.runner.flink.GraphApp;
import ideal.sylph.runner.flink.utils.FlinkJobUtil;
import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.File;

import static com.google.common.base.MoreObjects.toStringHelper;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

@Name("streamSql")
@Description("this is stream sql Actuator")
public class StreamSqlActuator
        implements JobActuator
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSqlActuator.class);
    private final YarnConfiguration configuration;

    @Inject
    public StreamSqlActuator(
            YarnConfiguration configuration
    )
    {
        this.configuration = configuration;
    }

    @NotNull
    @Override
    public Job formJob(File jobDir, Flow flow)
    {
        try {
            final FlinkApp app = new GraphApp(flow);
            return FlinkJobUtil.createJob("streamSql", jobDir, app, flow);
        }
        catch (Exception e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }
    }

    @Override
    public void execJob(Job job)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public String toString()
    {
        toStringHelper(this)
                .add("name", "streamSql")
                .add("description", ".....");
        throw new UnsupportedOperationException("this method have't support!");
    }
}
