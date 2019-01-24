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

import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.io.IOUtils;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.harbby.gadtry.base.Checks.checkState;
import static com.google.common.base.Preconditions.checkArgument;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_CONFIG_ERROR;
import static java.util.Objects.requireNonNull;

public class WebAppProxyServlet
        extends HttpServlet
{
    private static final Logger LOG = LoggerFactory.getLogger(WebAppProxyServlet.class);

    private static final Set<String> passThroughHeaders =
            new HashSet<>(Arrays.asList(
                    "User-Agent",
                    "Accept",
                    "Accept-Encoding",
                    "Accept-Language",
                    "Accept-Charset"));
    public static final String PROXY_USER_COOKIE_NAME = "proxy-user";

    private SylphContext sylphContext;

    @Override
    public void init(ServletConfig config)
            throws ServletException
    {
        super.init(config);
        this.sylphContext = ((SylphContext) getServletContext().getAttribute("sylphContext"));
    }

    /**
     * Download link and have it be the response.
     *
     * @param req the http request
     * @param resp the http response
     * @param link the link to download
     * @param cookie the cookie to set if any
     * @throws IOException on any error.
     */
    private static void proxyLink(HttpServletRequest req,
            HttpServletResponse resp, URI link, Cookie cookie, String proxyHost)
            throws IOException
    {
        DefaultHttpClient client = new DefaultHttpClient();
        client
                .getParams()
                .setParameter(ClientPNames.COOKIE_POLICY,
                        CookiePolicy.BROWSER_COMPATIBILITY)
                .setBooleanParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, true);
        // Make sure we send the request from the proxy address in the config
        // since that is what the AM filter checks against. IP aliasing or
        // similar could cause issues otherwise.
        InetAddress localAddress = InetAddress.getByName(proxyHost);
        if (LOG.isDebugEnabled()) {
            LOG.debug("local InetAddress for proxy host: {}", localAddress);
        }
        client.getParams()
                .setParameter(ConnRoutePNames.LOCAL_ADDRESS, localAddress);
        HttpGet httpGet = new HttpGet(link);
        @SuppressWarnings("unchecked")
        Enumeration<String> names = req.getHeaderNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            if (passThroughHeaders.contains(name)) {
                String value = req.getHeader(name);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("REQ HEADER: {} : {}", name, value);
                }
                httpGet.setHeader(name, value);
            }
        }

        String user = req.getRemoteUser();
        if (user != null && !user.isEmpty()) {
            httpGet.setHeader("Cookie",
                    PROXY_USER_COOKIE_NAME + "=" + URLEncoder.encode(user, "ASCII"));
        }
        OutputStream out = resp.getOutputStream();
        try {
            HttpResponse httpResp = client.execute(httpGet);
            resp.setStatus(httpResp.getStatusLine().getStatusCode());
            for (Header header : httpResp.getAllHeaders()) {
                resp.setHeader(header.getName(), header.getValue());
            }
            if (cookie != null) {
                resp.addCookie(cookie);
            }
            InputStream in = httpResp.getEntity().getContent();
            if (in != null) {
                IOUtils.copyBytes(in, out, 4096, true);
            }
        }
        finally {
            httpGet.releaseConnection();
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException
    {
        try {
            this.doGet1(req, resp);
        }
        catch (Exception e) {
            resp.sendError(500, Throwables.getRootCause(e).toString());
        }
    }

    protected void doGet1(HttpServletRequest req, HttpServletResponse resp)
            throws IOException
    {
        try {
            final String remoteUser = req.getRemoteUser();
            final String pathInfo = req.getPathInfo();

            String[] parts = pathInfo.split("/", 3);
            checkArgument(parts.length >= 2, remoteUser + " gave an invalid proxy path " + pathInfo);
            //parts[0] is empty because path info always starts with a /
            String runId = requireNonNull(parts[1], "runId not setting");
            String rest = parts.length > 2 ? parts[2] : "";

            URI trackingUri = new URI(getJobUrl(runId));

            // Append the user-provided path and query parameter to the original
            // tracking url.
            String query = req.getQueryString() == null ? "" : req.getQueryString();
            List<NameValuePair> queryPairs = URLEncodedUtils.parse(query, null);
            UriBuilder builder = UriBuilder.fromUri(trackingUri);
            for (NameValuePair pair : queryPairs) {
                builder.queryParam(pair.getName(), pair.getValue());
            }
            URI toFetch = builder.path(rest).build();

            proxyLink(req, resp, toFetch, null, null);
        }
        catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    public String getJobUrl(String id)
    {
        JobContainer container = sylphContext.getJobContainer(id)
                .orElseGet(() -> sylphContext.getJobContainerWithRunId(id).orElseThrow(() ->
                        new SylphException(JOB_CONFIG_ERROR, "job " + id + " not Online"))
                );
        Job.Status status = container.getStatus();
        checkState(status == Job.Status.RUNNING, "job " + id + " Status " + status + ",but not RUNNING");

        return container.getJobUrl();
    }
}
