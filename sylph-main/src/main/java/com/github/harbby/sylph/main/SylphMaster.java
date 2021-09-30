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
package com.github.harbby.sylph.main;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import com.github.harbby.gadtry.GadTry;
import com.github.harbby.gadtry.base.Platform;
import com.github.harbby.sylph.colltroller.AuthAspect;
import com.github.harbby.sylph.colltroller.ControllerApp;
import com.github.harbby.sylph.main.server.SylphBean;
import com.github.harbby.sylph.main.service.JobEngineManager;
import com.github.harbby.sylph.main.service.JobManager;
import com.github.harbby.sylph.main.service.OperatorManager;
import com.github.harbby.sylph.main.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static java.util.Objects.requireNonNull;

public final class SylphMaster
{
    private SylphMaster() {}

    private static final String LOGO = """
            *_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*
            |     Welcome to         __          __   ______    |
            |  .     _____  __  __  / / ____    / /_  \\ \\ \\ \\   |
            | /|\\   / ___/ / / / / / / / __ \\  / __ \\  \\ \\ \\ \\  |
            |( | ) _\\__ \\ / /_/ / / / / /_/ / / / / /   ) ) ) ) |
            | \\|/ /____/  \\__, / /_/ / .___/ /_/ /_/   / / / /  |
            |  '         /____/     /_/               /_/_/_/   |
            |  :: Sylph ::  version = (v1.0.0-SNAPSHOT)         |
            |  ::       :: spark.version = 3.2.0                |
            |  ::       :: flink.version = 1.4.3                |
            *---------------------------------------------------*""".indent(0);

    public static void main(String[] args)
            throws Exception
    {
        var logger = loadConfig(SylphMaster.class);
        System.out.println(LOGO);
        var configFile = System.getProperty("config");
        var sylphBean = new SylphBean(PropertiesUtil.loadProperties(new File(configFile)));

        /*2 Initialize GadTry Injector */
        try {
            logger.info("========={} Bootstrap initialize...========", SylphMaster.class.getCanonicalName());
            var app = GadTry.create(sylphBean, binder ->
                    binder.bind(ControllerApp.class).withSingle()
            ).aop(new AuthAspect()).initialize();
            //----analysis
            logger.info("Analyzed App dependencies {}", String.join("\n", app.analyze().printShow()));
            app.getInstance(JobEngineManager.class).loadRunners();
            app.getInstance(OperatorManager.class).loadPlugins();
            app.getInstance(JobManager.class).start();
            app.getInstance(ControllerApp.class).start();
            logger.info("======== SERVER STARTED this pid is {}========", Platform.getCurrentProcessId());
        }
        catch (Throwable e) {
            logger.error("SERVER START FAILED...", e);
            System.exit(-1);
        }
    }

    /**
     * 加载外部的logback配置文件
     */
    private static Logger loadConfig(Class<?> maicClass)
            throws JoranException
    {
        String logbackXml = requireNonNull(System.getProperty("logging.config"), "logback not setting");

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(logbackXml);
        StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
        return LoggerFactory.getLogger(maicClass);
    }
}
