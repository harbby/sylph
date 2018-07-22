package ideal.sylph.runner.flink.utils;

import com.google.common.collect.ImmutableSet;
import ideal.sylph.common.jvm.JVMLauncher;
import ideal.sylph.common.jvm.JVMLaunchers;
import ideal.sylph.common.jvm.VmFuture;
import ideal.sylph.runner.flink.FlinkApp;
import ideal.sylph.runner.flink.FlinkJob;
import ideal.sylph.runner.flink.FlinkRunner;
import ideal.sylph.runner.flink.JobParameter;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;

public final class FlinkJobUtil
{
    private FlinkJobUtil() {}

    private static final Logger logger = LoggerFactory.getLogger(FlinkJobUtil.class);

    public static Job createJob(
            String actuatorName, String jobId, FlinkApp app, Flow flow)
            throws Exception
    {
        List<URL> userJars = getAppClassLoaderJars(app);
        //---------编译job-------------
        JobGraph jobGraph = compile(app, 2, userJars);
        //----------------设置状态----------------
        JobParameter state = new JobParameter()
                .queue("default")
                .taskManagerCount(2) //-yn 注意是executer个数
                .taskManagerMemoryMb(1024) //1024mb
                .taskManagerSlots(1) // -ys
                .jobManagerMemoryMb(1024) //-yjm
                .appTags(ImmutableSet.of("a1", "a2"))
                .setUserProvidedJar(getUserAdditionalJars(userJars))
                .setYarnJobName("sylph_" + jobId);

        return FlinkJob.newJob()
                .setJobParameter(state)
                .setJobGraph(jobGraph)
                .setId(jobId)
                .setActuatorName(actuatorName)
                .setDescription("this is flink stream job...")
                .setFlow(flow)
                .build();
    }

    /**
     * 对job 进行编译
     */
    private static JobGraph compile(FlinkApp flinkApp, int parallelism, List<URL> userProvidedJars)
            throws Exception
    {
        JVMLauncher<JobGraph> launcher = JVMLaunchers.<JobGraph>newJvm()
                .setCallable(() -> {
                    System.out.println("************ job start ***************");
                    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
                    execEnv.setParallelism(parallelism);
                    flinkApp.build(execEnv);
                    return execEnv.getStreamGraph().getJobGraph();
                })
                .addUserjars(userProvidedJars)
                .build();
        VmFuture<JobGraph> result = launcher.startAndGet(flinkApp.getClassLoader());
        return result.get().orElseThrow(() -> new SylphException(JOB_BUILD_ERROR, result.getOnFailure()));
    }

    private static List<URL> getAppClassLoaderJars(FlinkApp flinkApp)
    {
        ImmutableList.Builder<URL> builder = ImmutableList.builder();
        final ClassLoader appClassLoader = flinkApp.getClassLoader();
        if (appClassLoader instanceof URLClassLoader) {
            builder.add(((URLClassLoader) appClassLoader).getURLs());

            final ClassLoader parentClassLoader = appClassLoader.getParent();
            if (parentClassLoader instanceof URLClassLoader) {
                builder.add(((URLClassLoader) parentClassLoader).getURLs());
            }
        }
        return builder.build();
    }

    /**
     * 获取任务额外需要的jar
     */
    private static Iterable<Path> getUserAdditionalJars(List<URL> userJars)
    {
        return userJars.stream().map(jar -> {
            try {
                final URI uri = jar.toURI();
                final File file = new File(uri);
                if (file.exists() && file.isFile()) {
                    return new Path(uri);
                }
            }
            catch (Exception e) {
                logger.warn("add user jar error with URISyntaxException {}", jar);
            }
            return null;
        }).filter(x -> Objects.nonNull(x) && !x.getName().startsWith(FlinkRunner.FLINK_DIST)).collect(Collectors.toList());
    }
}
