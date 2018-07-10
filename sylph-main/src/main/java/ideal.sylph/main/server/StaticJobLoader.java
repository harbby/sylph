package ideal.sylph.main.server;

import com.google.inject.Inject;
import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.RunnerManger;
import ideal.sylph.spi.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static ideal.sylph.main.util.PropertiesUtil.loadProperties;
import static java.util.Objects.requireNonNull;

public class StaticJobLoader
{
    private static final Logger logger = LoggerFactory.getLogger(StaticJobLoader.class);

    private final JobManager jobManager;
    private final RunnerManger runnerManger;

    @Inject
    public StaticJobLoader(
            JobManager jobManager,
            RunnerManger runnerManger
    )
    {
        this.jobManager = requireNonNull(jobManager, "jobManager is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger is null");
    }

    /**
     * load local jobs dir job
     */
    public void loadJobs()
    {
        File jobsDir = new File("jobs");
        Stream.of(requireNonNull(jobsDir.listFiles(), "jobs Dir is not exists"))
                .parallel()
                .forEach(jobDir -> {
                    try {
                        final var typeFile = new File(jobDir, "type.job");
                        checkArgument(typeFile.exists() && typeFile.isFile(), typeFile + " is not exists or isDirectory");
                        var jobProps = loadProperties(typeFile);
                        Job job = runnerManger.formJobWithDir(jobDir, jobProps);
                        jobManager.saveJob(job);
                    }
                    catch (Exception e) {
                        logger.warn("job加载失败:{}", jobDir, e);
                    }
                });
    }
}
