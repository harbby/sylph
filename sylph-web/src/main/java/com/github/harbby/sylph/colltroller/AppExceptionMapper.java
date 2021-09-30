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
package com.github.harbby.sylph.colltroller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.jvm.JVMException;
import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AppExceptionMapper
        extends Exception
        implements ExceptionMapper<Exception>
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(AppExceptionMapper.class);

    @Override
    public Response toResponse(Exception ex)
    {
        String fullError = ex instanceof JVMException ? ex.getMessage() :
                Throwables.getStackTraceAsString(ex);
        Map<String, Object> result = ImmutableMap.<String, Object>builder()
                .put("success", false)
                .put("error_code", "001")
                .put("message", fullError)
                .build();
        logger.warn("", ex);
        return Response.status(200)
                .entity(result).type(MediaType.APPLICATION_JSON).build();
    }
}
