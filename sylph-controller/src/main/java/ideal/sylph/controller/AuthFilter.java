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

import ideal.sylph.controller.action.LoginController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import java.io.IOException;

public class AuthFilter
        implements ContainerRequestFilter
{
    private @Context HttpServletRequest request;
    private @Context UriInfo uriInfo;

    @Override
    public void filter(ContainerRequestContext requestContext)
            throws IOException
    {
        HttpSession session = request.getSession();
        LoginController.User user = (LoginController.User) session.getAttribute("user");

        if (uriInfo.getRequestUri().getPath().startsWith("/_sys/auth") ||
                uriInfo.getRequestUri().getPath().startsWith("/_sys/server")) {
            return;
        }

        if (user == null) {
            throw new RuntimeException("Need to login again");
        }
    }
}
