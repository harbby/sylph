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
package com.github.harbby.sylph.runner.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkUtil
{
    private SparkUtil() {}

    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingSqlEngine.class);

    public static SparkSession getSparkSession(boolean isCompile)
    {
        logger.info("========create SparkSession with mode.isCompile = " + isCompile + "============");
        SparkConf sparkConf = isCompile ?
                new SparkConf().setMaster("local[*]").setAppName("sparkCompile").set("spark.ui.enabled", "false")
                : new SparkConf();
        return SparkSession.builder().config(sparkConf).getOrCreate();
    }
}
