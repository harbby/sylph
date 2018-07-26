package ideal.sylph.runner.spark.yarn;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import ideal.sylph.runner.spark.SparkJob;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.spark.ideal.deploy.yarn.SylphSparkYarnClient;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.stream.Collectors;

public class SparkAppLauncher
{
    @Inject private YarnClient yarnClient;

    public ApplicationId run(SparkJob job)
            throws Exception
    {
        final String sparkHome = System.getenv("SPARK_HOME");  //获取环境变量
        System.setProperty("SPARK_YARN_MODE", "true");
        //
        SparkConf sparkConf = new SparkConf();
        sparkConf.setSparkHome(sparkHome);

        sparkConf.setMaster("yarn");
        sparkConf.setAppName("sylph_" + job.getId());
        sparkConf.set("spark.submit.deployMode", "cluster"); // worked
        //------------addJars-> --jars    ------------  上传依赖的jar文件
        String additionalJars = getAppClassLoaderJars().stream()
                .map(URL::getPath).filter(x -> {
                    File file = new File(x);
                    return file.isFile() && !file.getPath().startsWith(sparkHome);
                })
                .collect(Collectors.joining(","));
        if (additionalJars != null && additionalJars.length() > 0) {
            sparkConf.set("spark.yarn.dist.jars", additionalJars);
        }

        //-------------addFiles->  --files  ----------------------------
//        File[] userFiles = loadDir.listFiles();
//        if (userFiles != null) {
//            String files = Arrays.stream(userFiles)
//                    .filter(File::isFile).map(File::getAbsolutePath).collect(Collectors.joining(","));
//            if (files != null && files.length() > 0) {
//                sparkConf.set("spark.yarn.dist.files", files);   //上传配置文件
//            }
//        }

        String[] args = getArgs();
        ClientArguments clientArguments = new ClientArguments(args);                 // spark-2.0.0

        yarnClient.getConfig().iterator().forEachRemaining(x -> {
            sparkConf.set("spark.hadoop." + x.getKey(), x.getValue());
        });
        Client appClient = new SylphSparkYarnClient(clientArguments, sparkConf, yarnClient);
        return appClient.submitApplication();
    }

    private String[] getArgs()
    {
        return new String[] {
                //"--name",
                //"test-SparkPi",

                //"--driver-memory",
                //"1000M",

                //"--jar", sparkExamplesJar,

                "--class", "ideal.sylph.runner.spark.SparkAppUtil",

                // argument 1 to my Spark program
                //"--arg", slices   用户自定义的参数

                // argument 2 to my Spark program (helper argument to create a proper JavaSparkContext object)
                //"--arg",
        };
    }

    /**
     *
     */
    @Deprecated
    private List<URL> getAppClassLoaderJars()
    {
        //URLClassLoader classLoader = (URLClassLoader) JobBuilder.class.getClassLoader();
        ImmutableList.Builder<URL> builder = ImmutableList.builder();
        final ClassLoader appClassLoader = this.getClass().getClassLoader();
        if (appClassLoader instanceof URLClassLoader) {
            builder.add(((URLClassLoader) appClassLoader).getURLs());

            final ClassLoader parentClassLoader = appClassLoader.getParent();
            if (parentClassLoader instanceof URLClassLoader) {
                builder.add(((URLClassLoader) parentClassLoader).getURLs());
            }
        }
        return builder.build();
    }
}
