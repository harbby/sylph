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

        if (uriInfo.getRequestUri().getPath().startsWith("/_sys/auth")) {
            return;
        }

        if (user == null) {
            throw new RuntimeException("Need to login again");
        }
    }
}
