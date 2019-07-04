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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BufferedHeader;
import org.eclipse.jetty.http.HttpStatus;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Enumeration;

public class ProxyUtil
{
    private ProxyUtil() {}

    public static final String PROXY_USER_COOKIE_NAME = "proxy-user";

    public static void proxyLink(HttpServletRequest req,
            HttpServletResponse resp, URI link, String proxyServerHost)
            throws IOException, URISyntaxException
    {
        HttpRequestBase httpRequest = createHttpRequest(req, link);
        Enumeration<String> names = req.getHeaderNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            String value = req.getHeader(name);
            httpRequest.setHeader(name, value);
        }

        String user = req.getRemoteUser();
        if (user != null && !user.isEmpty()) {
            httpRequest.setHeader("Cookie",
                    PROXY_USER_COOKIE_NAME + "=" + URLEncoder.encode(user, "ASCII"));
        }

        InetAddress localAddress = InetAddress.getByName(proxyServerHost);
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setLocalAddress(localAddress)
                .setCircularRedirectsAllowed(true)
                .build();

        try (CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpResponse httpResp = client.execute(httpRequest);
            resp.setStatus(httpResp.getStatusLine().getStatusCode());
            for (Header header : httpResp.getAllHeaders()) {
                resp.setHeader(header.getName(), header.getValue());
            }

            if (httpResp.getStatusLine().getStatusCode() == HttpStatus.FOUND_302) {
                BufferedHeader header = (BufferedHeader) httpResp.getFirstHeader("Location");
                proxyLink(req, resp, new URI(header.getValue()), null);
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

    private static HttpRequestBase createHttpRequest(HttpServletRequest req, URI link)
            throws IOException
    {
        switch (req.getMethod().toUpperCase()) {
            case HttpGet.METHOD_NAME:
                return new HttpGet(link);
            case HttpPost.METHOD_NAME: {
                HttpPost httpPost = new HttpPost(link);
                InputStreamEntity inputStreamEntity = new InputStreamEntity(req.getInputStream());
                httpPost.setEntity(inputStreamEntity);
                return httpPost;
            }
            case HttpPut.METHOD_NAME: {
                HttpPut httpPost = new HttpPut(link);
                InputStreamEntity inputStreamEntity = new InputStreamEntity(req.getInputStream());
                httpPost.setEntity(inputStreamEntity);
                return httpPost;
            }
            case HttpDelete.METHOD_NAME:
                return new HttpDelete(link);
            case HttpHead.METHOD_NAME:
                return new HttpHead(link);
            default:
                throw new UnsupportedOperationException("The " + req.getMethod() + " have't support!");
        }
    }
}
