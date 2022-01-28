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

import com.github.harbby.gadtry.aop.AopBinder;
import com.github.harbby.gadtry.aop.Aspect;
import com.github.harbby.sylph.colltroller.action.LoginController;
import com.github.harbby.sylph.spi.SylphContext;
import jakarta.servlet.http.HttpSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthAspect
        implements Aspect
{
    private static final Logger logger = LoggerFactory.getLogger(AuthAspect.class);
    static final ThreadLocal<HttpSession> SESSION_THREAD_LOCAL = new ThreadLocal<>();

    @Override
    public void register(AopBinder binder)
    {
        binder.bind(SylphContext.class)
                .doAround(proxy -> {
                    HttpSession session = SESSION_THREAD_LOCAL.get();
                    String user = session == null ? null : ((LoginController.User) session.getAttribute("user")).getUserName();
                    String action = proxy.getName();
                    if (!"getJob".equals(proxy.getName())) {
                        logger.info("[auth] user:{}, action: {}, args: {}", user, action, proxy.getArgs());
                    }
                    return proxy.proceed();
                }).whereMethod(method -> !"getJobContainer".equals(method.getName()));
    }
}
