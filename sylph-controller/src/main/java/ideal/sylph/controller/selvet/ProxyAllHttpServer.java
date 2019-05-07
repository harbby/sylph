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
package ideal.sylph.controller.selvet;

import com.github.harbby.gadtry.ioc.Autowired;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static ideal.sylph.controller.utils.ProxyUtil.proxyLink;

public class ProxyAllHttpServer
{
    private static final Logger logger = LoggerFactory.getLogger(ProxyAllHttpServer.class);
    private final int proxyHttpPort;

    @Autowired
    public ProxyAllHttpServer(Properties properties)
    {
        this.proxyHttpPort = Integer.parseInt(properties.getProperty("web.proxy.port", "28080"));
    }

    public void start()
            throws Exception
    {
        int maxFormContentSize = 100;

        // 创建Server
        Server server = new Server(proxyHttpPort);
        server.setAttribute("org.eclipse.jetty.server.Request.maxFormContentSize",
                maxFormContentSize);

        ServletContextHandler contextHandler = new ServletContextHandler(
                ServletContextHandler.NO_SESSIONS);
        contextHandler.setContextPath("/");
        contextHandler.addServlet(ProxyAll.class, "/*");

        server.setHandler(contextHandler);

        server.start();
        logger.info("Web proxy Server started... the port " + proxyHttpPort);
    }

    public static class ProxyAll
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException
        {
            try {
                proxyLink(req, resp, new URI(req.getRequestURL().toString()), null, null);
            }
            catch (URISyntaxException e) {
                throw new IOException(e);
            }
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException
        {
            this.doGet(req, resp);
        }
    }
}
