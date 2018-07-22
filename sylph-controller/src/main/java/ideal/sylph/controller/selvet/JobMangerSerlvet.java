package ideal.sylph.controller.selvet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.common.base.Throwables;
import ideal.sylph.controller.SylphServlet;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.job.JobContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

import static ideal.sylph.spi.job.Job.Status.STOP;
import static java.util.Objects.requireNonNull;

@WebServlet(urlPatterns = "/_sys/job_manger")
public class JobMangerSerlvet
        extends SylphServlet
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static Logger logger = LoggerFactory.getLogger(JobMangerSerlvet.class);
    private static final long serialVersionUID = 33213425435L;

    private SylphContext sylphContext;

    @Override
    public void init(ServletConfig config)
            throws ServletException
    {
        super.init(config);
        this.sylphContext = ((SylphContext) getServletContext().getAttribute("sylphContext"));
    }

    public static String getBodyString(HttpServletRequest request)
            throws IOException
    {
        //-获取post body--注意要放到--request.getParameter 之前 否则可能失效
        final StringBuilder stringBuilder = new StringBuilder(2000);
        try (Scanner scanner = new Scanner(request.getInputStream())) {
            while (scanner.hasNextLine()) {
                stringBuilder.append(scanner.nextLine());
            }
            String bodyStr = stringBuilder.toString().trim();
            if (bodyStr.equals("")) {
                bodyStr = "{}";
            }
            return bodyStr;
        }
    }

    @Override
    protected void doPostHandler(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException
    {
        final String bodyStr = getBodyString(request);
        Body body;
        String type = request.getParameter("type");
        if ("add".equals(type)) {
            body = new Body(type, "jobid");
        }
        else {
            body = MAPPER.readValue(bodyStr, Body.class);
        }

        String out = null;
        switch (body.getType()) {
            case "list":  //获取列表
                out = listJobs();
                break;
            case "stop":  //下线应用
                sylphContext.stopJob(body.getJobId());
                break;
            case "active": //启动任务
                sylphContext.startJob(body.getJobId());
                break;
            case "delete": //删除任务
                sylphContext.deleteJob(body.getJobId());
                break;
            case "refresh_all":  //刷新
                out = listJobs();
                break;
            default:
                break;
        }

        response.getWriter().println(out);
    }

    private String listJobs()
            throws IOException
    {
        final List<Object> outData = new ArrayList<>();
        String json = null;
        try {
            sylphContext.getAllJobs().forEach(job -> {
                String jobId = job.getId();
                Optional<JobContainer> jobContainer = sylphContext.getJobContainer(jobId);

                Map<String, Object> line = new HashMap<>();
                line.put("status", STOP);  //默认为未上线
                line.put("jobId", jobId);
                line.put("type", job.getActuatorName());
                line.put("create_time", 0);  //getUserModuleManger().getCount("action")

                jobContainer.ifPresent(container -> {
                    line.put("yarnId", container.getRunId());
                    line.put("status", container.getStatus());
                    line.put("app_url", "/proxy/" + jobId + "/#");
                });
                outData.add(line);
            });
            json = MAPPER.writeValueAsString(ImmutableMap.of("data", outData));
        }
        catch (Exception e) {
            logger.error("", Throwables.getRootCause(e));
        }

        return json;
    }

    private static final class Body
    {
        private final String type;
        private final String jobId;

        @JsonCreator
        public Body(
                @JsonProperty("type") String type,
                @JsonProperty("jobId") String jobId)
        {
            this.type = requireNonNull(type, "type must not null");
            this.jobId = jobId;
        }

        @JsonProperty
        public String getJobId()
        {
            return requireNonNull(jobId, "jobId must not null");
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }
    }
}
