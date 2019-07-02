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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.controller.action.LoginController;
import ideal.sylph.controller.selvet.WebAppProxyServlet;
import ideal.sylph.spi.SylphContext;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpSession;

import java.io.IOException;
import java.util.Map;

import static ideal.sylph.controller.AuthAspect.SESSION_THREAD_LOCAL;
import static java.util.Objects.requireNonNull;

/**
 * Created by ideal on 17-3-15.
 */
public final class JettyServer
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);
    private Server server;
    private final ServerConfig serverConfig;
    private final SylphContext sylphContext;
    private final LogAppender logAppender = new LogAppender(2000);

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
        int jettyPort = serverConfig.getServerPort();
        int maxFormContentSize = serverConfig.getMaxFormContentSize();

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
        ServletHolder servlet = new ServletHolder(new ServletContainer(new WebApplication()))
        {
            @Override
            protected void prepare(Request baseRequest, ServletRequest request, ServletResponse response)
                    throws ServletException, UnavailableException
            {
                SESSION_THREAD_LOCAL.set(baseRequest.getSession(true));
                super.prepare(baseRequest, request, response);
            }

            @Override
            public void handle(Request baseRequest, ServletRequest request, ServletResponse response)
                    throws ServletException, UnavailableException, IOException
            {
                HttpSession session = baseRequest.getSession();
                LoginController.User user = (LoginController.User) session.getAttribute("user");

                if (baseRequest.getRequestURI().startsWith("/_sys/auth") ||
                        baseRequest.getRequestURI().startsWith("/_sys/server")) {
                    super.handle(baseRequest, request, response);
                    return;
                }
                if (user == null) {
                    response.setContentType("application/json");
                    Map<String, Object> result = ImmutableMap.<String, Object>builder()
                            .put("success", false)
                            .put("error_code", "001")
                            .put("message", "Need to login again")
                            .build();
                    response.getWriter().println(MAPPER.writeValueAsString(result));
                    return;
                }
                super.handle(baseRequest, request, response);
            }
        };
        //1M = 1048576
        servlet.getRegistration().setMultipartConfig(new MultipartConfigElement("data/tmp", 1048576_00, 1048576_00, 262144));

        //--------------------plblic----------------------
        ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);  //NO_SESSIONS
        contextHandler.setContextPath("/");
        contextHandler.setAttribute("sylphContext", sylphContext);
        contextHandler.setAttribute("logAppender", logAppender);

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
