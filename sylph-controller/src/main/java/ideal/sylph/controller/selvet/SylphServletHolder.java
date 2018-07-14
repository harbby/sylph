package ideal.sylph.controller.selvet;

import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.MultipartConfigElement;
import javax.servlet.Servlet;

public class SylphServletHolder
        extends ServletHolder
{
    public SylphServletHolder(Servlet servlet)
    {
        super(servlet);
        final MultipartConfigElement element = new MultipartConfigElement(
                "/tmp/sylph/jobs",
                1024 * 1024 * 100,
                1024 * 1024 * 100,
                262144
        );
        this.getRegistration().setMultipartConfig(element);
    }
}
