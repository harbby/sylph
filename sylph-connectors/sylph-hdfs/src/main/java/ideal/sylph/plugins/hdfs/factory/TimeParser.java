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
package ideal.sylph.plugins.hdfs.factory;

import ideal.sylph.plugins.hdfs.utils.CommonUtil;
import org.joda.time.DateTime;

import java.util.UUID;

/**
 * TimeParser 解析time 生成partionPath 和key等
 * <p>
 * 如果不喜欢样式 可以自己实现该类
 **/

public class TimeParser
{
    private final DateTime eventTime;

    public TimeParser(DateTime eventTime)
    {
        this.eventTime = eventTime;
    }

    public TimeParser(Long eventTime)
    {
        this(new DateTime(eventTime));
    }

    public String getPartionDay()
    {
        return eventTime.toString("yyyyMMdd");
    }

    public String getPartionMinute()
    {
        StringBuilder key = new StringBuilder(eventTime.toString("HH"));
        String minute = eventTime.toString("mm");
        key.append(minute.charAt(0));
        key.append(Integer.parseInt(minute.charAt(1) + "") / 5 * 5);
        return key.toString();
    }

    public String getWriterKey()
    {
        return getPartionDay() + getPartionMinute();
    }

    public String getFileName()
    {
        String ip = CommonUtil.getDefaultIpOrPid();
        //"/_tmp_" + this.getPartionMinute + "_" + ip + "_" + UUID.randomUUID().toString
        return new StringBuilder("/_tmp_").append(this.getPartionMinute())
                .append("_").append(ip).append("_").append(UUID.randomUUID().toString())
                .toString();
    }

    public String getPartitionPath()
    {
        //"/day="+getPartionDay+"/minute="+getPartionMinute +"/"+ getFileName
        return new StringBuilder("day=").append(getPartionDay()).append("/minute=")
                .append(getPartionMinute()).append(getFileName()).toString();
    }
}
