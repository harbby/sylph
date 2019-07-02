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
package ideal.sylph.runner.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;

import java.io.IOException;

public class SparkJobConfigTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public void testConfigParser()
            throws IOException
    {
        String confString = "{\n" +
                "    \"type\": \"SparkJobConfigTest\",\n" +
                "    \"config\": {\n" +
                "        \"driver-memory\": \"1024m\",\n" +
                "        \"driver-cores\": 1,\n" +
                "        \"num-executors\": 2,\n" +
                "        \"executor-memory\": \"1024m\",\n" +
                "        \"executor-cores\": 1,\n" +
                "        \"queue\": \"default\",\n" +
                "        \"sparkConf\": {}\n" +
                "    }\n" +
                "}";
        SparkJobConfig config = MAPPER.readValue(confString, SparkJobConfig.class);
        String writeValueAsString = MAPPER.writeValueAsString(config);
        Assert.assertNotNull(writeValueAsString);
    }
}