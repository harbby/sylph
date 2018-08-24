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
package ideal.sylph.runner.batch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TestQuartzScheduling
{
    private final static Logger logger = LoggerFactory.getLogger(TestQuartzScheduling.class);
    private Scheduler scheduler;

    @Before
    public void setUp()
            throws Exception
    {
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        scheduler = schedulerFactory.getScheduler();
        scheduler.start();
        Assert.assertTrue(scheduler.isStarted());
    }

    @After
    public void tearDown()
            throws Exception
    {
        scheduler.shutdown();
        Assert.assertTrue(scheduler.isShutdown());
    }

    @Test
    public void testAddJob()
            throws SchedulerException
    {
        JobDetail jobDetail = JobBuilder.newJob(HelloQuartzJob.class)
                .withIdentity("testJob", Scheduler.DEFAULT_GROUP)
                .build();

        String cronExpression = "30/5 * * * * ?"; // 每分钟的30s起，每5s触发任务

        CronTrigger cronTrigger = TriggerBuilder.newTrigger()
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();

        scheduler.scheduleJob(jobDetail, cronTrigger);

        Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.anyGroup());
        Assert.assertEquals(1, jobKeys.size());
        logger.info("job list:{}",jobKeys);
    }

    @Test
    public void testDelete()
            throws InterruptedException, SchedulerException
    {
        TimeUnit.SECONDS.sleep(1);
        logger.info("remove job delayDataSchdule");
        scheduler.deleteJob(JobKey.jobKey("testJob"));
        Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.anyGroup());
        Assert.assertEquals(0, jobKeys.size());
    }

    public static class HelloQuartzJob
            implements Job
    {
        @Override
        public void execute(JobExecutionContext context)
                throws JobExecutionException
        {
            logger.info("HelloQuartzJob ....FireTime:{} , ScheduledFireTime:{}", context.getFireTime(), context.getScheduledFireTime());
        }
    }
}
