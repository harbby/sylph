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

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

public class WebApplication
        extends ResourceConfig
{
    public WebApplication()
    {
        //The jackson feature and provider is used for object serialization
        //between client and server objects in to a json
//        register(JacksonFeature.class);
//        register(JacksonProvider.class);

        register(AppExceptionMapper.class);
        //Glassfish multipart file uploader feature
        register(MultiPartFeature.class);
        register(AuthFilter.class);

        packages("ideal.sylph.controller.action");
    }
}
