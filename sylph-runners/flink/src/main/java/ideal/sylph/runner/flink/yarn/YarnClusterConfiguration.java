package ideal.sylph.runner.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Set;

public class YarnClusterConfiguration
{
    /**
     * The configuration used by YARN (i.e., <pre>yarn-site.xml</pre>).
     */
    private final YarnConfiguration conf;

    /**
     * The home directory of all job where all the temporary files for each jobs are stored.
     */
    private final String appRootDir;

    /**
     * The location of the Flink Uber jar.
     */
    private final Path flinkJar;

    /**
     * Additional resources to be localized for both JobManager and TaskManager.
     * They will NOT be added into the classpaths.
     */
    private final Set<Path> resourcesToLocalize;

    /**
     * JARs that will be localized and put into the classpaths for bot JobManager and TaskManager.
     */
    private final Set<Path> systemJars;

    /**
     * flink conf
     */
    private final Configuration flinkConfiguration = new Configuration();

    public YarnClusterConfiguration(
            YarnConfiguration conf,
            String appRootDir,
            Path flinkJar,
            Set<Path> resourcesToLocalize,
            Set<Path> systemJars)
    {
        this.conf = conf;
        this.appRootDir = appRootDir;
        this.flinkJar = flinkJar;
        this.resourcesToLocalize = resourcesToLocalize;
        this.systemJars = systemJars;
    }

    YarnConfiguration conf()
    {
        return conf;
    }

    public String appRootDir()
    {
        return appRootDir;
    }

    public Configuration flinkConfiguration()
    {
        return flinkConfiguration;
    }

    public Path flinkJar()
    {
        return flinkJar;
    }

    public Set<Path> resourcesToLocalize()
    {
        return resourcesToLocalize;
    }

    public Set<Path> systemJars()
    {
        return systemJars;
    }
}
