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
package ideal.sylph.spi.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static ideal.sylph.spi.job.JobContainer.Status.RUNNING;
import static ideal.sylph.spi.job.JobContainer.Status.STARTED_ERROR;
import static ideal.sylph.spi.job.JobContainer.Status.STOP;

public abstract class JobContainerAbs
        implements JobContainer
{
    private static final Logger logger = LoggerFactory.getLogger(JobContainer.class);
    private int countReStart;
    private long lastStartTime;
    private volatile Status status = STOP;

    public int getCountReStart()
    {
        return countReStart;
    }

    public long getLastStartTime()
    {
        return lastStartTime;
    }

    protected abstract String deploy()
            throws Exception;

    @Override
    public final Optional<String> run()
            throws Exception
    {
        this.countReStart++;
        long startTime = System.currentTimeMillis();
        if ((startTime - getLastStartTime()) < 300_000) {
            logger.warn("STARTED_ERROR, Job restarts in a short time");
            this.setStatus(STARTED_ERROR);
            return Optional.empty();
        }
        else {
            this.lastStartTime = startTime;
            String runId = deploy();
            this.setStatus(RUNNING);
            return Optional.of(runId);
        }
    }

    @Override
    public void setStatus(Status status)
    {
        this.status = status;
    }

    @Override
    public Status getStatus()
    {
        return status;
    }
}
