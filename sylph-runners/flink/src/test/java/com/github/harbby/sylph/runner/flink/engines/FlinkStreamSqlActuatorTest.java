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
package com.github.harbby.sylph.runner.flink.engines;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FlinkStreamSqlActuatorTest
{
    @Test
    public void testSqlSplit()
            throws URISyntaxException, IOException
    {
        String sqlFilePath = this.getClass().getResource("/stream_test.sql").getPath();
        String sql = FileUtils.readFileToString(new File(sqlFilePath), UTF_8);

        String[] split = sql.split(";(?=([^\']*\'[^\']*\')*[^\']*$)");
        Assert.assertEquals(4, split.length);
    }
}