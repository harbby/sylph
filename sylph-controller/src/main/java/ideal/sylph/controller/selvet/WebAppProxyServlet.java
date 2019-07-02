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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobContainer;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.google.common.base.Preconditions.checkArgument;
import static ideal.sylph.controller.utils.ProxyUtil.proxyLink;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_CONFIG_ERROR;
import static java.util.Objects.requireNonNull;

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
            this.doGet1(req, resp);
        }
        catch (Exception e) {
            resp.sendError(500, Throwables.getStackTraceAsString(e));
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
            String jobIdOrRunId = requireNonNull(parts[1], "jobId or runId not setting");
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

            proxyLink(req, resp, toFetch, null, null);
        }
        catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    private final Cache<String, String> urlCache = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .maximumSize(100)
            .build();

    public String getJobUrl(String jobIdOrRunId)
            throws IOException
    {
        String url = urlCache.getIfPresent(jobIdOrRunId);
        if (url != null) {
            return url;
        }

        JobContainer container = sylphContext.getJobContainer(jobIdOrRunId)
                .orElseThrow(() -> new SylphException(JOB_CONFIG_ERROR, "job " + jobIdOrRunId + " not Online"));
        Job.Status status = container.getStatus();
        checkState(status == Job.Status.RUNNING, "job " + jobIdOrRunId + " Status " + status + ",but not RUNNING");

        urlCache.put(container.getRunId(), container.getJobUrl());
        return container.getJobUrl();
    }
}
