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

import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.jvm.JVMException;
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.Strings.isNotBlank;
import static ideal.sylph.spi.exception.StandardErrorCode.ILLEGAL_OPERATION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@javax.inject.Singleton
@Path("/stream_sql")
public class StreamSqlResource
{
    private static final Logger logger = LoggerFactory.getLogger(EtlResource.class);

    private final UriInfo uriInfo;
    private final SylphContext sylphContext;

    public StreamSqlResource(
            @Context ServletContext servletContext,
            @Context UriInfo uriInfo)
    {
        this.uriInfo = uriInfo;
        this.sylphContext = (SylphContext) servletContext.getAttribute("sylphContext");
    }

    /**
     * save job
     */
    @POST
    @Path("save")
    @Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Map saveJob(@Context HttpServletRequest request)
    {
        String jobId = null;
        try {
            jobId = requireNonNull(request.getParameter("jobId"), "job jobId is not empty");
            String flow = request.getParameter("query");
            String configString = request.getParameter("config");
            checkState(isNotBlank(jobId), "JobId IS NULL");
            checkState(isNotBlank(flow), "SQL query IS NULL");
            sylphContext.saveJob(jobId, flow, ImmutableMap.of("type", "StreamSql", "config", parserJobConfig(configString)));
            Map out = ImmutableMap.of(
                    "jobId", jobId,
                    "type", "save",
                    "status", "ok",
                    "msg", "ok");
            logger.info("save job {}", jobId);
            return out;
        }
        catch (JVMException e) {
            logger.warn("save job {} failed: {}", jobId, e.getMessage());
            String message = e.getMessage();
            return ImmutableMap.of("type", "save",
                    "status", "error",
                    "msg", message);
        }
        catch (Exception e) {
            logger.warn("save job {} failed: {}", jobId, e);
            String message = Throwables.getStackTraceAsString(e);
            return ImmutableMap.of("type", "save",
                    "status", "error",
                    "msg", message);
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

        File userFilesDir = new File(job.getWorkDir(), "files");
        File[] userFiles = userFilesDir.listFiles();
        List<String> files = userFilesDir.exists() && userFiles != null ?
                Arrays.stream(userFiles).map(File::getName).collect(Collectors.toList())
                : Collections.emptyList();

        return ImmutableMap.builder()
                .put("query", job.getFlow().toString())
                .put("config", job.getConfig())
                .put("msg", "Get job successfully")
                .put("status", "ok")
                .put("files", files)
                .put("jobId", jobId)
                .build();
    }

    static Map parserJobConfig(String configString)
            throws IOException
    {
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(configString.getBytes(UTF_8)));
        String appTags = properties.getProperty("appTags", null);
        if (appTags != null) {
            String[] tags = appTags.split(",");
            properties.put("appTags", tags);
        }

        return ImmutableMap.copyOf(properties);
    }
}
