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
package ideal.sylph.plugins.hdfs.txt;

import ideal.sylph.plugins.hdfs.factory.TimeParser;
import ideal.sylph.plugins.hdfs.utils.CommonUtil;
import org.joda.time.DateTime;

public class TextTimeParser
        extends TimeParser
{
    public TextTimeParser(DateTime eventTime)
    {
        super(eventTime);
    }

    public TextTimeParser(Long eventTime)
    {
        super(eventTime);
    }

    @Override
    public String getFileName()
    {
        String ip = CommonUtil.getDefaultIpOrPid();
        return "/_tmp_" + this.getPartionMinute() + "_" + ip + "_" + CommonUtil.getProcessID();
    }

    @Override
    public String getWriterKey()
    {
        return this.getPartionDay();
    }

    @Override
    public String getPartitionPath()
    {
        return this.getPartionDay() + this.getFileName();
    }
}
