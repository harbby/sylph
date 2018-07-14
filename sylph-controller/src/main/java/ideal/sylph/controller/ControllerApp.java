package ideal.sylph.controller;

import com.google.inject.Inject;
import ideal.sylph.spi.SylphContext;

import static java.util.Objects.requireNonNull;

/**
 * 视图层目前 为实验功能
 */
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
        new JettyServer(config, sylphContext).start();
    }
}
