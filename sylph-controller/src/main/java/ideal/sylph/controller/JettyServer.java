/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.sylph.controller;

import ideal.sylph.controller.selvet.WebAppProxyServlet;
import ideal.sylph.spi.SylphContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.MultipartConfigElement;

import static java.util.Objects.requireNonNull;

/**
 * Created by ideal on 17-3-15.
 */
@Deprecated
public final class JettyServer
{
    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);
    private Server server;
    private final ServerConfig serverConfig;
    private final SylphContext sylphContext;

    JettyServer(
            ServerConfig serverConfig,
            SylphContext sylphContext
    )
    {
        this.serverConfig = requireNonNull(serverConfig, "serverConfig is null");
        this.sylphContext = requireNonNull(sylphContext, "sylphContext is null");
    }

    public void start()
            throws Exception
    {
        //-------初始化------获取Context句柄------
        int jettyPort = serverConfig.getServerPort();
        int maxFormContentSize = serverConfig.getMaxFormContentSize();

        // 创建Server
        this.server = new Server(jettyPort);
        server.setAttribute("org.eclipse.jetty.server.Request.maxFormContentSize",
                maxFormContentSize);

        HandlerList handlers = loadHandlers();  //加载路由
        server.setHandler(handlers);
        logger.info("web Server started... the port {}", jettyPort);
        server.start();
    }

    private HandlerList loadHandlers()
    {
        HandlerList handlers = new HandlerList();
        ServletHolder servlet = new ServletHolder(new ServletContainer(new WebApplication()));
        servlet.getRegistration().setMultipartConfig(new MultipartConfigElement("data/tmp", 1048576, 1048576, 262144));

        //--------------------plblic----------------------
        ServletContextHandler contextHandler = new ServletContextHandler(
                ServletContextHandler.NO_SESSIONS);
        contextHandler.setContextPath("/");
        contextHandler.setAttribute("sylphContext", sylphContext);

        //-------add jersey--------
        contextHandler.addServlet(servlet, "/_sys/*");
        contextHandler.addServlet(WebAppProxyServlet.class, "/proxy/*");

        final ServletHolder staticServlet = new ServletHolder(new DefaultServlet());
        contextHandler.addServlet(staticServlet, "/css/*");
        contextHandler.addServlet(staticServlet, "/js/*");
        contextHandler.addServlet(staticServlet, "/images/*");
        contextHandler.addServlet(staticServlet, "/fonts/*");
        contextHandler.addServlet(staticServlet, "/favicon.ico");
        contextHandler.addServlet(staticServlet, "/");
        contextHandler.setResourceBase("webapp");

        handlers.addHandler(contextHandler);
        return handlers;
    }
}
