package ideal.sylph.controller;

import ideal.sylph.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class AppExceptionMapper
        extends Exception
        implements ExceptionMapper<Exception>
{
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(AppExceptionMapper.class);

    @Override
    public Response toResponse(Exception ex)
    {
        return Response.status(404).entity(Throwables.getStackTraceAsString(ex)).type("text/plain").build();
    }
}
