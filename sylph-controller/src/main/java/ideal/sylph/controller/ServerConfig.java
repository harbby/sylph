package ideal.sylph.controller;

import io.airlift.configuration.Config;

import javax.validation.constraints.Min;

public class ServerConfig
{
    private int serverPort = 8080;

    @Config("server.http.port")
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
}
