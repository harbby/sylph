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

import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class MemoryUtil
{
    private MemoryUtil() {}

    private static final Logger log = Logger.getLogger(MemoryUtil.class);
    /**
     * 内存正常使用量保护值, 低于该值则撤销内存过载控制 70%
     */
    private static final double autoTrigerGcThreshold = 0.70;

    /**
     * 当监测线程观察到内存占比到达85%时, 程序几乎卡住不动(GC线程跑满)
     * 内存已严重过载，即将触发full gc
     */
    //public static final double memoryOverLoadThreshold = 0.85;

//    public static volatile long lastGcTime = 0;

    /**
     * 检查当前内存占用情况, 并执行相应的动作
     */
    public static boolean checkMemory()
    {
        Runtime run = Runtime.getRuntime();
        long max = run.maxMemory();  //jvm
        long total = run.totalMemory(); // 已申请的
        long free = run.freeMemory();  // 申请后剩余的空间
        long usable = max - total + free;
        double occupyRatio = 1 - (double) usable / (double) max;

        // 超过70%, 打印内存信息
        if (occupyRatio >= autoTrigerGcThreshold) {
            //lastGcTime = System.currentTimeMillis();
            log.warn("当前内存明细:");
            log.warn("最大内存 = " + max / 1024 / 1024 + "M字节");
            log.warn("已分配内存 = " + total / 1024 / 1024 + "M字节");
            log.warn("已分配内存中的剩余空间 = " + free / 1024 / 1024 + "M字节");
            log.warn("最大可用内存 = " + usable / 1024 / 1024 + "M字节");
            log.warn("内存使用比 = " + occupyRatio);
            log.warn("=========================================\n");
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * 获取当前内存概况
     */
    public static String getMemoryInfo(String host)
    {
        Runtime run = Runtime.getRuntime();
        long max = run.maxMemory();  //jvm
        long total = run.totalMemory(); // 已申请的
        long free = run.freeMemory();  // 申请后剩余的空间
        long usable = max - total + free;
        double occupyRatio = 1 - (double) usable / (double) max;
        String info = String.format("%s 最大内存 = %sM字节 当前使用比率:%s", host, run.maxMemory() / 1024 / 1024, occupyRatio);
        return info;
    }

    public static void checkMemoryAwaitTermination()
            throws InterruptedException
    {
        while (checkMemory() == true) {
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
