package ideal.sylph.controller;

import io.airlift.configuration.Config;

import javax.validation.constraints.Min;

public class ServerConfig
{
    private int serverPort = 8080;
    private int maxFormContentSize = 100;

    @Config("web.server.port")
    public ServerConfig setServerPort(int serverPort)
    {
        this.serverPort = serverPort;
        return this;
    }

    @Min(1000)
    public int getServerPort()
    {
        return serverPort;
    }

    @Config("server.http.maxFormContentSize")
    public ServerConfig setMaxFormContentSize(int maxFormContentSize)
    {
        this.maxFormContentSize = maxFormContentSize;
        return this;
    }

    @Min(10)
    public int getMaxFormContentSize()
    {
        return maxFormContentSize;
    }
}
