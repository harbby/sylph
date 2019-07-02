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
package ideal.sylph.controller;

import com.github.harbby.gadtry.base.Throwables;
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
        logger.warn("", ex);
        return Response.status(404).entity(Throwables.getStackTraceAsString(Throwables.getRootCause(ex))).type("text/plain").build();
    }
}
