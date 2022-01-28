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

import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.sylph.api.annotation.Description;
import com.github.harbby.sylph.api.annotation.Name;
import com.github.harbby.sylph.spi.CompileJobException;
import com.github.harbby.sylph.spi.job.JobConfig;
import com.github.harbby.sylph.spi.job.JobEngine;
import com.github.harbby.sylph.spi.job.JobParser;
import com.github.harbby.sylph.spi.job.SqlJobParser;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.function.Supplier;

import static com.github.harbby.sylph.runner.spark.SQLHepler.buildSql;
import static java.util.Objects.requireNonNull;

@Name("StructuredStreamingSql")
@Description("this is spark structured streaming sql Actuator")
public class StructuredStreamingSqlEngine
        implements JobEngine
{
    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingSqlEngine.class);

    @Override
    public JobParser analyze(String flowBytes)
    {
        return SqlJobParser.parser(flowBytes);
    }

    @Override
    public Serializable compileJob(JobParser inJobParser, JobConfig jobConfig)
            throws Exception
    {
        return compileJob0((SqlJobParser) inJobParser);
    }

    private static Serializable compileJob0(SqlJobParser parser)
            throws Exception
    {
        SparkSession sparkSession = new StructuredStreamingSqlJobCompler(true, parser).get();
        requireNonNull(sparkSession, "sparkSession is null");
        return new StructuredStreamingSqlJobCompler(false, parser);
    }

    private static class StructuredStreamingSqlJobCompler
            implements Serializable, Supplier<org.apache.spark.sql.SparkSession>
    {
        private static final long serialVersionUID = -2478310899466429291L;
        private final boolean isCompile;
        private final SqlJobParser parser;

        private StructuredStreamingSqlJobCompler(boolean isCompile, SqlJobParser parser)
        {
            this.isCompile = isCompile;
            this.parser = parser;
        }

        @Override
        public SparkSession get()
        {
            SparkSession sparkSession = SparkUtil.getSparkSession(isCompile);
            SqlAnalyse sqlAnalyse = new StructuredStreamingSqlAnalyse(sparkSession, isCompile);
            try {
                buildSql(sqlAnalyse, parser);
            }
            catch (Exception e) {
                throw new CompileJobException("spark job compile failed", e);
            }
            return sparkSession;
        }
    }

    @Override
    public List<Class<?>> keywords()
    {
        return ImmutableList.of(
                org.apache.spark.sql.SparkSession.class,
                org.apache.spark.sql.Dataset.class,
                org.apache.spark.sql.Row.class);
    }
}
