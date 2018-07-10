package ideal.sylph.runner.flink;

import io.airlift.configuration.Config;

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import java.io.File;

public class FlinkRunnerConfig
{
    private int serverPort = 8080;
    final File flinkJarFile = getFlinkJarFile();

    @Config("server.http.port")
    public FlinkRunnerConfig setServerPort(int serverPort)
    {
        this.serverPort = serverPort;
        return this;
    }

    @Min(1000)
    public int getServerPort()
    {
        return serverPort;
    }

    @Nonnull
    public File getFlinkJarFile()
    {
        return flinkJarFile;
    }
}
