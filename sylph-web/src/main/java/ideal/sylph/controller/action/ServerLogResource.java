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

import ideal.sylph.controller.LogAppender;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.Map;

@javax.inject.Singleton
@Path("/server")
public class ServerLogResource
{
    private final LogAppender logAppender;

    public ServerLogResource(
            @Context ServletContext servletContext)
    {
        this.logAppender = (LogAppender) servletContext.getAttribute("logAppender");
    }

    @GET
    @Path("/logs")
    @Produces({MediaType.APPLICATION_JSON})
    public Map<String, Object> getServerLog(@QueryParam(value = "id") String groupId,
            @QueryParam(value = "last_num") int next)
    {
        return logAppender.readLines(groupId, next);
    }
}
