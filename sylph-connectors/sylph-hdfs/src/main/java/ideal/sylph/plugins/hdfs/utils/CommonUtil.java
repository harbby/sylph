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
package ideal.sylph.plugins.hdfs.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class CommonUtil
{
    private CommonUtil() {}

    public static String getHostName()
            throws UnknownHostException
    {
        return InetAddress.getLocalHost().getHostName();
    }

    public static String getDefaultIp()
            throws UnknownHostException
    {
        return InetAddress.getLocalHost().getHostAddress();
    }

    public static int getProcessID()
    {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return Integer.valueOf(runtimeMXBean.getName().split("@")[0]);
    }

    public static String getHostNameOrPid()
    {
        try {
            return getHostName();
        }
        catch (UnknownHostException e) {
            return String.valueOf(getProcessID());
        }
    }

    public static String getDefaultIpOrPid()
    {
        try {
            return getDefaultIp();
        }
        catch (UnknownHostException e) {
            return String.valueOf(getProcessID());
        }
    }
}
