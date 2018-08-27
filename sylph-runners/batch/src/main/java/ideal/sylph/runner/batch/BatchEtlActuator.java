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

import com.google.inject.Inject;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.common.graph.Graph;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobContainerAbs;
import ideal.sylph.spi.job.JobHandle;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;

import javax.validation.constraints.NotNull;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Name("BatchEtl123")
@Name("BatchEtl")
@Description("batch Etl job")
public class BatchEtlActuator
        implements JobActuatorHandle
{
    @Inject private Scheduler batchJobScheduler;

    @Override
    public JobHandle formJob(String jobId, Flow flow, DirClassLoader jobClassLoader)
    {
        return new JobHandle() {};
    }

    @Override
    public JobContainer createJobContainer(@NotNull Job job, String jobInfo)
    {
        final Graph graph = GraphUtils.getGraph(job.getId(), job.getFlow());
        String cronExpression = "30/5 * * * * ?"; // 每分钟的30s起，每5s触发任务
        return new JobContainerAbs()
        {
            @Override
            public String getRunId()
            {
                return "UNKNOWN";
            }

            @Override
            public boolean isRunning()
            {
                try {
                    return batchJobScheduler.checkExists(JobKey.jobKey(job.getId()));
                }
                catch (SchedulerException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Optional<String> run()
                    throws SchedulerException
            {
                // online job with batch scheduler 添加到周期任务调度器中
                JobDetail jobDetail = JobBuilder.newJob(QuartzJob.class)
                        .withIdentity(job.getId(), Scheduler.DEFAULT_GROUP)
                        .build();
                jobDetail.getJobDataMap().put("graph", graph);

                CronTrigger cronTrigger = TriggerBuilder.newTrigger()
                        .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                        .build();
                batchJobScheduler.scheduleJob(jobDetail, cronTrigger);
                return Optional.of(job.getId());
            }

            @Override
            public void shutdown()
            {
                try {
                    batchJobScheduler.deleteJob(JobKey.jobKey(job.getId()));
                }
                catch (SchedulerException e) {
                    throw new RuntimeException("Offline job failed", e);
                }
            }

            @Override
            public String getJobUrl()
            {
                return "UNKNOWN";
            }
        };
    }

    private class QuartzJob
            implements org.quartz.Job
    {
        @Override
        public void execute(JobExecutionContext context)
                throws JobExecutionException
        {
            JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
            final Graph graph = (Graph) requireNonNull(jobDataMap.get("graph"), "graph is null");
            try {
                graph.run();
            }
            catch (Exception e) {
                throw new JobExecutionException(e);
            }
        }
    }
}
