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
package ideal.sylph.controller.action;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.ImmutableMap;
import lombok.Data;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@Path("/auth")
@javax.inject.Singleton
public class LoginController
{
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Data
    public static class User
    {
        private String userName;
        private String password;
    }

    @Path("/login")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Map doLogin(User user, @Context HttpServletRequest req)
    {
        HttpSession session = req.getSession();
        User sessionUser = (User) session.getAttribute("user");
        if (sessionUser != null) {
            return ImmutableMap.builder()
                    .put("message", "login ok")
                    .put("userName", sessionUser.getUserName())
                    .put("success", true)
                    .build();
        }

        //1...check user
        requireNonNull(user, "user is null");
        if ("admin".equals(user.getUserName()) && "admin".equals(user.getPassword())) {

            session.setMaxInactiveInterval(30 * 60);
            session.setAttribute("user", user);
            return ImmutableMap.builder()
                    .put("message", "login ok")
                    .put("userName", user.getUserName())
                    .put("success", true)
                    .build();
        }
        return ImmutableMap.builder()
                .put("message", "login failed")
                .put("userName", user.getUserName())
                .put("success", false)
                .build();
    }

    @Path("/logout")
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public boolean doLogout(@Context HttpServletRequest req)
    {
        HttpSession session = req.getSession(); //获取当前session
        if (session != null) {
            User user = (User) session.getAttribute("user"); //从当前session中获取用户信息
            session.invalidate(); //关闭session
        }
        return true;
    }
}
