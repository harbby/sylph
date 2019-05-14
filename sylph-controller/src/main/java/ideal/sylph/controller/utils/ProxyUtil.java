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
package ideal.sylph.controller.utils;

import com.github.harbby.gadtry.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BufferedHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public class ProxyUtil
{
    private ProxyUtil() {}

    public static final Set<String> PASS_THROUGH_HEADERS =
            new HashSet<>(Arrays.asList(
                    "User-Agent",
                    "Accept",
                    "Accept-Encoding",
                    "Accept-Language",
                    "Accept-Charset",
                    "Content-Type",
                    "Origin",
                    "Access-Control-Request-Method",
                    "Access-Control-Request-Headers"));
    public static final String PROXY_USER_COOKIE_NAME = "proxy-user";

    private static final Logger logger = LoggerFactory.getLogger(ProxyUtil.class);

    public static void proxyLink(HttpServletRequest req,
            HttpServletResponse resp, URI link, Cookie cookie, String proxyHost)
            throws IOException, URISyntaxException
    {
        HttpRequestBase httpRequest;
        if ("GET".equalsIgnoreCase(req.getMethod())) {
            httpRequest = new HttpGet(link);
        }
        else if ("POST".equalsIgnoreCase(req.getMethod())) {
            HttpPost httpPost = new HttpPost(link);
            InputStreamEntity inputStreamEntity = new InputStreamEntity(req.getInputStream());
            httpPost.setEntity(inputStreamEntity);
            httpRequest = httpPost;
        }
        else {
            throw new UnsupportedOperationException("The " + req.getMethod() + " have't support!");
        }

        @SuppressWarnings("unchecked")
        Enumeration<String> names = req.getHeaderNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            if (PASS_THROUGH_HEADERS.contains(name)) {
                String value = req.getHeader(name);
                if (logger.isDebugEnabled()) {
                    logger.debug("REQ HEADER: {} : {}", name, value);
                }
                httpRequest.setHeader(name, value);
            }
        }

        String user = req.getRemoteUser();
        if (user != null && !user.isEmpty()) {
            httpRequest.setHeader("Cookie",
                    PROXY_USER_COOKIE_NAME + "=" + URLEncoder.encode(user, "ASCII"));
        }

        try (CloseableHttpClient client = HttpClients.createMinimal()) {
            client.getParams()
                    .setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.BROWSER_COMPATIBILITY)
                    .setBooleanParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, true);
            // Make sure we send the request from the proxy address in the config
            // since that is what the AM filter checks against. IP aliasing or
            // similar could cause issues otherwise.
            InetAddress localAddress = InetAddress.getByName(proxyHost);
            client.getParams().setParameter(ConnRoutePNames.LOCAL_ADDRESS, localAddress);

            HttpResponse httpResp = client.execute(httpRequest);
            resp.setStatus(httpResp.getStatusLine().getStatusCode());
            for (Header header : httpResp.getAllHeaders()) {
                resp.setHeader(header.getName(), header.getValue());
            }
            if (cookie != null) {
                resp.addCookie(cookie);
            }

            if (httpResp.getStatusLine().getStatusCode() == HttpStatus.FOUND_302) {
                BufferedHeader header = (BufferedHeader) httpResp.getFirstHeader("Location");
                proxyLink(req, resp, new URI(header.getValue()), null, null);
            }
            else {
                InputStream in = httpResp.getEntity().getContent();
                if (in != null) {
                    IOUtils.copyBytes(in, resp.getOutputStream(), 4096, true);
                }
            }
        }
        finally {
            httpRequest.releaseConnection();
        }
    }
}
