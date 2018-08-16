package ideal.sylph.controller.action;

import com.google.common.collect.ImmutableMap;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import java.util.Map;
import java.util.Optional;

import static ideal.sylph.spi.exception.StandardErrorCode.ILLEGAL_OPERATION;
import static java.util.Objects.requireNonNull;

@javax.inject.Singleton
@Path("/etl_builder")
public class EtlResource
{
    private static final Logger logger = LoggerFactory.getLogger(EtlResource.class);

    private final UriInfo uriInfo;
    private final SylphContext sylphContext;

    public EtlResource(
            @Context ServletContext servletContext,
            @Context UriInfo uriInfo)
    {
        this.uriInfo = uriInfo;
        this.sylphContext = (SylphContext) servletContext.getAttribute("sylphContext");
    }

    /**
     * 保存job
     */
    @POST
    @Path("save")
    @Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Map saveJob(@Context HttpServletRequest request)
    {
        try {
            String jobId = requireNonNull(request.getParameter("jobId"), "job jobId 不能为空");
            String flow = request.getParameter("graph");
            sylphContext.saveJob(jobId, flow, "StreamETL");
            Map out = ImmutableMap.of(
                    "jobId", jobId,
                    "type", "save",
                    "status", "ok",
                    "msg", "编译过程:..."
            );
            logger.info("save job {}", jobId);
            return ImmutableMap.copyOf(out);
        }
        catch (Exception e) {
            Map out = ImmutableMap.of("type", "save",
                    "status", "error",
                    "msg", "任务创建失败: " + e.toString()
            );
            logger.warn("job 创建失败", e);
            return ImmutableMap.copyOf(out);
        }
    }

    /**
     * 编辑job
     */
    @GET
    @Path("get")
    @Produces({MediaType.APPLICATION_JSON})
    public Map getJob(@QueryParam("jobId") String jobId)
    {
        requireNonNull(jobId, "jobId is null");
        Optional<Job> jobOptional = sylphContext.getJob(jobId);
        Job job = jobOptional.orElseThrow(() -> new SylphException(ILLEGAL_OPERATION, "job " + jobId + " not found"));

        return ImmutableMap.builder()
                .put("graph", job.getFlow())
                .put("msg", "获取任务成功")
                .put("status", "ok")
                .put("jobId", jobId)
                .build();
    }
}
