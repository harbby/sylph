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
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import java.util.List;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.Strings.isNotBlank;

/**
 *
 */
@javax.inject.Singleton
@Path("/job_manger")
public class JobManagerResource
{
    private static final Logger logger = LoggerFactory.getLogger(JobManagerResource.class);

    private final ServletContext servletContext;
    private final UriInfo uriInfo;
    private final SylphContext sylphContext;

    public JobManagerResource(
            @Context ServletContext servletContext,
            @Context UriInfo uriInfo)
    {
        this.servletContext = servletContext;
        this.uriInfo = uriInfo;
        this.sylphContext = (SylphContext) servletContext.getAttribute("sylphContext");
    }

    @GET
    @Path("/jobs")
    @Produces({MediaType.APPLICATION_JSON})
    public List<JobInfo> listJobs()
    {
        return sylphContext.getAllJobs();
    }

    @GET
    @Path("/job/{id}")
    @Produces({MediaType.APPLICATION_JSON})
    public JobInfo getJob(@PathParam("id") int jobId)
    {
        return sylphContext.getJob(jobId);
    }

    /**
     * save job
     */
    @POST
    @Path("save")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public boolean saveJob(JobStore.DbJob dbJob)
            throws Exception
    {
        checkState(isNotBlank(dbJob.getJobName()), "job name is Empty");
        checkState(isNotBlank(dbJob.getType()), "job type is Empty");
        checkState(isNotBlank(dbJob.getQueryText()), "job query text is Empty");

        sylphContext.saveJob(dbJob);
        logger.info("save job {} success.", dbJob.getJobName());
        return true;
    }

    @GET
    @Path("/stop/{id}")
    @Produces({MediaType.APPLICATION_JSON})
    public boolean stopJob(@PathParam("id") int id)
    {
        sylphContext.stopJob(id);
        return true;
    }

    @GET
    @Path("/delete/{id}")
    @Produces({MediaType.APPLICATION_JSON})
    public boolean deleteJob(@PathParam("id") int id)
    {
        sylphContext.deleteJob(id);
        return true;
    }

    @GET
    @Path("/deploy/{id}")
    @Produces({MediaType.APPLICATION_JSON})
    public boolean deployJob(@PathParam("id") int id)
    {
        sylphContext.startJob(id);
        return true;
    }
}
