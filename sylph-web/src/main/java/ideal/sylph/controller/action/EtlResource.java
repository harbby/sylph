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

import com.google.common.collect.ImmutableMap;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.job.JobStore;
import ideal.sylph.spi.model.JobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
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

import static java.util.Objects.requireNonNull;

@javax.inject.Singleton
@Path("/etl_builder")
public class EtlResource
{
    private static final Logger logger = LoggerFactory.getLogger(EtlResource.class);
    private final SylphContext sylphContext;

    public EtlResource(@Context ServletContext servletContext)
    {
        this.sylphContext = (SylphContext) servletContext.getAttribute("sylphContext");
    }

    /**
     * 保存job
     */
    @POST
    @Path("save")
    @Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public void saveJob(JobStore.DbJob dbJob)
            throws Exception
    {
        requireNonNull(dbJob.getType(), "job type is null");
        requireNonNull(dbJob.getJobName(), "job name is null");
        sylphContext.saveJob(dbJob);
    }

    /**
     * 编辑job
     */
    @GET
    @Path("get")
    @Produces({MediaType.APPLICATION_JSON})
    public Map getJob(@QueryParam("jobId") int jobId)
    {
        requireNonNull(jobId, "jobId is null");
        JobInfo job = sylphContext.getJob(jobId);
        return ImmutableMap.builder()
                .put("graph", job.getQueryText())
                .put("config", job.getConfig())
                .put("msg", "获取任务成功")
                .put("status", "ok")
                .put("jobId", jobId)
                .build();
    }
}
