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
package com.github.harbby.sylph.colltroller.action;

import com.github.harbby.sylph.spi.SylphContext;
import com.github.harbby.sylph.spi.dao.Job;
import com.github.harbby.sylph.spi.dao.JobInfo;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.Strings.isNotBlank;

@jakarta.inject.Singleton
@Path("/job_manger")
public class JobManagerResource
{
    private static final Logger logger = LoggerFactory.getLogger(JobManagerResource.class);

    private final SylphContext sylphContext;

    public JobManagerResource(@Context ServletContext servletContext)
    {
        this.sylphContext = (SylphContext) servletContext.getAttribute("sylphContext");
    }

    @GET
    @Path("/jobs")
    @Produces(MediaType.APPLICATION_JSON)
    public List<JobInfo> listJobs()
    {
        return sylphContext.getAllJobs();
    }

    @GET
    @Path("/job/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public JobInfo getJob(@PathParam("id") int jobId)
    {
        return sylphContext.getJob(jobId);
    }

    /**
     * save job
     */
    @POST
    @Path("save")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public boolean saveJob(Job job)
            throws Exception
    {
        checkState(isNotBlank(job.getJobName()), "job name is Empty");
        checkState(isNotBlank(job.getType()), "job type is Empty");
        checkState(isNotBlank(job.getQueryText()), "job query text is Empty");

        sylphContext.saveJob(job);
        logger.info("save job {} success.", job.getJobName());
        return true;
    }

    @GET
    @Path("/stop/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public boolean stopJob(@PathParam("id") int id)
    {
        sylphContext.stopJob(id);
        return true;
    }

    @GET
    @Path("/delete/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public boolean deleteJob(@PathParam("id") int id)
    {
        sylphContext.deleteJob(id);
        return true;
    }

    @GET
    @Path("/deploy/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public boolean deployJob(@PathParam("id") int id)
            throws Exception
    {
        sylphContext.deployJob(id);
        return true;
    }
}
