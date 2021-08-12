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
package ideal.sylph.main;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import com.github.harbby.gadtry.GadTry;
import com.github.harbby.gadtry.ioc.Bean;
import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.controller.AuthAspect;
import ideal.sylph.controller.ControllerApp;
import ideal.sylph.main.server.SylphBean;
import ideal.sylph.main.service.JobEngineManager;
import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.OperatorManager;
import ideal.sylph.main.util.PropertiesUtil;
import ideal.sylph.spi.job.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static java.util.Objects.requireNonNull;

public final class SylphMaster
{
    private SylphMaster() {}

    private static final Logger logger = LoggerFactory.getLogger(SylphMaster.class);
    private static final String LOGO = "\n *_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*\n" +
            " |     Welcome to         __          __   ______    |\n" +
            " |  .     _____  __  __  / / ____    / /_  \\ \\ \\ \\   |\n" +
            " | /|\\   / ___/ / / / / / / / __ \\  / __ \\  \\ \\ \\ \\  |\n" +
            " |( | ) _\\__ \\ / /_/ / / / / /_/ / / / / /   ) ) ) ) |\n" +
            " | \\|/ /____/  \\__, / /_/ / .___/ /_/ /_/   / / / /  |\n" +
            " |  '         /____/     /_/               /_/_/_/   |\n" +
            " |  :: Sylph ::  version = (v0.8.0-SNAPSHOT)         |\n" +
            " *---------------------------------------------------*";

    public static void main(String[] args)
            throws Exception
    {
        //PropertyConfigurator.configure(requireNonNull(System.getProperty("log4j.file"), "log4j.file not setting"));
        loadConfig(requireNonNull(System.getProperty("logging.config"), "logback not setting"));
        String configFile = System.getProperty("config");
        Bean sylphBean = new SylphBean(PropertiesUtil.loadProperties(new File(configFile)));

        /*2 Initialize GadTry Injector */
        try {
            logger.info("========={} Bootstrap initialize...========", SylphMaster.class.getCanonicalName());
            IocFactory app = GadTry.create(sylphBean, binder ->
                    binder.bind(ControllerApp.class).withSingle()
            ).aop(new AuthAspect()).initialize();
            //----analysis
            logger.info("Analysis App dependencys {}", String.join("\n", app.analysis().printShow()));

            app.getInstance(OperatorManager.class).loadPlugins();
            app.getInstance(JobEngineManager.class).loadRunners();
            app.getInstance(JobStore.class).loadJobs();

            app.getInstance(JobManager.class).start();
            app.getInstance(ControllerApp.class).start();
            //ProcessHandle.current().pid()
            logger.info(LOGO);
            logger.info("======== SERVER STARTED this pid is {}========");
        }
        catch (Throwable e) {
            logger.error("SERVER START FAILED...", e);
            System.exit(1);
        }
    }

    /**
     * 加载外部的logback配置文件
     */
    private static void loadConfig(String logbackXml)
            throws JoranException
    {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(logbackXml);
        StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
    }
}
