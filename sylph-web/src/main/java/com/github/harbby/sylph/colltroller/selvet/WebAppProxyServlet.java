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
package com.github.harbby.sylph.colltroller.selvet;

import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.sylph.spi.SylphContext;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.UriBuilder;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static com.github.harbby.sylph.colltroller.utils.ProxyUtil.proxyLink;
import static com.google.common.base.Preconditions.checkArgument;

public class WebAppProxyServlet
        extends HttpServlet
{
    private static final Logger logger = LoggerFactory.getLogger(WebAppProxyServlet.class);

    private SylphContext sylphContext;

    @Override
    public void init(ServletConfig config)
            throws ServletException
    {
        super.init(config);
        this.sylphContext = ((SylphContext) getServletContext().getAttribute("sylphContext"));
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException
    {
        try {
            this.doGet0(req, resp);
        }
        catch (Exception e) {
            resp.sendError(500, Throwables.getStackTraceAsString(e));
        }
    }

    protected void doGet0(HttpServletRequest req, HttpServletResponse resp)
            throws Exception
    {
        try {
            final String remoteUser = req.getRemoteUser();
            final String pathInfo = req.getPathInfo();

            String[] parts = pathInfo.split("/", 3);
            checkArgument(parts.length >= 2, remoteUser + " gave an invalid proxy path " + pathInfo);
            //parts[0] is empty because path info always starts with a /
            String jobIdOrRunId = parts[1];
            String rest = parts.length > 2 ? parts[2] : "";

            URI trackingUri = new URI(getJobUrl(jobIdOrRunId));

            // Append the user-provided path and query parameter to the original
            // tracking url.
            String query = req.getQueryString() == null ? "" : req.getQueryString();
            List<NameValuePair> queryPairs = URLEncodedUtils.parse(query, null);
            UriBuilder builder = UriBuilder.fromUri(trackingUri);
            for (NameValuePair pair : queryPairs) {
                builder.queryParam(pair.getName(), pair.getValue());
            }
            URI toFetch = builder.path(rest).build();

            proxyLink(req, resp, toFetch, null);
        }
        catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    public String getJobUrl(String jobIdOrRunId)
            throws Exception
    {
        return sylphContext.getJobWebUi(jobIdOrRunId);
    }
}
