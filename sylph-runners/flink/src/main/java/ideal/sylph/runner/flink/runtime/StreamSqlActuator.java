package ideal.sylph.runner.flink.runtime;

import com.google.inject.Inject;
import ideal.sylph.spi.Job;
import ideal.sylph.spi.JobActuator;
import ideal.sylph.spi.annotation.Description;
import ideal.sylph.spi.annotation.Name;
import ideal.sylph.spi.exception.SylphException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;

import static com.google.common.base.MoreObjects.toStringHelper;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

@Name("streamSql")
@Description("this is stream sql Actuator")
public class StreamSqlActuator
        implements JobActuator
{
    private final static Logger logger = LoggerFactory.getLogger(StreamSqlActuator.class);

    @Inject
    public StreamSqlActuator(
            YarnConfiguration configuration
    )
    {
    }

    @Nonnull
    @Override
    public Job formJob(File jobDir)
    {
        File yaml = new File(jobDir, "job.yaml");
        if (yaml.exists() && yaml.isFile()) {
            logger.info("加载任务:", yaml);
//                final QueryDagConf dagConf = QueryDagConf.load(yaml);  //解析Job定义文件
//                return loadJob(jobDir, dagConf);

        }
        else {
            throw new SylphException(JOB_BUILD_ERROR, yaml + " file is not exists");
        }

        throw new UnsupportedOperationException("this method have't support!");
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
