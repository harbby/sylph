package ideal.sylph.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

public abstract class SylphServlet
        extends HttpServlet
{
    protected static final Logger logger = LoggerFactory.getLogger(SylphServlet.class);

    @Override
    protected void doPost(
            final HttpServletRequest request,
            final HttpServletResponse response)
            throws ServletException, IOException
    {
        //针对post请求，设置允许接收中文
        request.setCharacterEncoding("UTF-8");
        response.setContentType("text/plain;charset=utf-8");
        response.setHeader("Access-Control-Allow-Origin", "*"); //允许跨域访问

        //-获取post body--注意要放到--request.getParameter 之前 否则可能失效
        this.doPostHandler(request, response);
    }

    protected abstract void doPostHandler(
            final HttpServletRequest request,
            final HttpServletResponse response
    )
            throws ServletException, IOException;
}
