package ideal.sylph.controller.selvet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.controller.SylphServlet;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.job.Job;

import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@WebServlet(urlPatterns = "/_sys/job_graph_edit")
@MultipartConfig
public class ETLJobServlet
        extends SylphServlet
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final SylphContext sylphContext;

    public ETLJobServlet(SylphContext sylphContext)
    {
        this.sylphContext = sylphContext;
    }

    @Override
    protected void doPostHandler(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        //----------------------------------------------
        String type = requireNonNull(request.getParameter("type"), "type 不能为空");

        switch (type) {
            case "save":
                saveJob(request, response);
                break;
            case "edit":
                getJob(request, response);
                break;
            default:
                logger.warn("type有误 {}", type);
        }
    }

    /**
     * 保存job
     */
    private void saveJob(HttpServletRequest request,
            HttpServletResponse response
    )
            throws IOException
    {
        try {
            String jobId = requireNonNull(request.getParameter("jobId"), "job jobId 不能为空");
            String flow = request.getParameter("graph");
            sylphContext.saveJob(jobId, flow, "StreamETL");
            Map out = ImmutableMap.of(
                    "jobId", jobId,
                    "type", "save",
                    "status", "ok",
                    "msg", "编译过程:dwadwad1312012\n12312\n4234fsekjfhesjk\n测试"
            );
            logger.info("save job {}", jobId);
            response.getWriter().println(MAPPER.writeValueAsString(out));
        }
        catch (Exception e) {
            Map out = ImmutableMap.of("type", "save",
                    "status", "error",
                    "msg", "任务创建失败: " + e.toString()
            );
            logger.warn("job 创建失败", e);
            response.getWriter().println(MAPPER.writeValueAsString(out));
        }
    }

    /**
     * 编辑job
     */
    private void getJob(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        String jobId = request.getParameter("jobId");
        Optional<Job> job = sylphContext.getJob(jobId);

        final Map<String, Object> out = new HashMap<>();
        if (job.isPresent()) {
            out.put("graph", job.get().getFlow());
            out.put("msg", "获取任务成功");
            out.put("status", "ok");
        }
        else {
            out.put("msg", "jobid:" + jobId + "不存在");
            out.put("status", "error");
        }
        out.put("jobId", jobId);
        response.getWriter().println(MAPPER.writeValueAsString(out));
    }
}
