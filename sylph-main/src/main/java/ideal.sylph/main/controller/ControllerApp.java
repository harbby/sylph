package ideal.sylph.main.controller;

import com.google.inject.Inject;
import ideal.sylph.main.SylphContext;
import ideal.sylph.main.server.ServerConfig;

import static java.util.Objects.requireNonNull;

public class ControllerApp
{
    private ServerConfig config;
    private SylphContext sylphContext;

    @Inject
    public ControllerApp(
            ServerConfig config,
            SylphContext sylphContext
    )
    {
        this.config = requireNonNull(config, "config is null");
        this.sylphContext = requireNonNull(sylphContext, "jobManager is null");
    }

    public void start()
            throws Exception
    {
        System.out.println("server.port: " + config.getServerPort());
    }
}
