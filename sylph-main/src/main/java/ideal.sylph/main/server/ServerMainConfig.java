package ideal.sylph.main.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class ServerMainConfig
{
    private String metadataPath;

    @Config("server.metadata.path")
    @ConfigDescription("server.metadata.path location")
    public ServerMainConfig setMetadataPath(String metadataPath)
    {
        this.metadataPath = metadataPath;
        return this;
    }

    @NotNull(message = "server.metadata.path not setting")
    public String getMetadataPath()
    {
        return metadataPath;
    }
}
