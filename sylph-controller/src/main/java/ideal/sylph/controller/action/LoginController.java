package ideal.sylph.controller.action;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ideal.sylph.spi.exception.StandardErrorCode;
import ideal.sylph.spi.exception.SylphException;
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

import static java.util.Objects.requireNonNull;

@Path("/auth")
@javax.inject.Singleton
public class LoginController
{
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Data
    public static class User
    {
        private String user;
        private String password;
    }

    @Path("/login")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public User doLogin(User user, @Context HttpServletRequest req)
    {
        HttpSession session = req.getSession();
        if (session.getAttribute("user") != null) {
            return (User) session.getAttribute("user");
        }

        //1...check user
        requireNonNull(user, "user is null");
        if (user.getUser().equals("admin") && user.getPassword().equals("admin")) {
            session.setMaxInactiveInterval(30 * 60);
            session.setAttribute("user", user);
            return user;
        }
        throw new SylphException(StandardErrorCode.NOT_SUPPORTED, "login error, Login failed, username or password is incorrect");
    }

    @Path("/logout")
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public boolean doLogout(@Context HttpServletRequest req)
    {
        HttpSession session = req.getSession();//获取当前session
        if (session != null) {
            User user = (User) session.getAttribute("user");//从当前session中获取用户信息
            session.invalidate();//关闭session
        }
        return true;
    }
}
