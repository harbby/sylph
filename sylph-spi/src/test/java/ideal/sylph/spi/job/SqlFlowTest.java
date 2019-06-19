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

import org.junit.Assert;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SqlFlowTest
{
    String query = "a1;a2;'12;34';\"a4;a8\";10";

    @Test
    public void getSqlSplit()
    {
        SqlFlow sqlFlow = SqlFlow.of(query.getBytes(UTF_8));
        Assert.assertArrayEquals(sqlFlow.getSqlSplit(), new String[] {"a1", "a2", "'12;34'", "\"a4;a8\"", "10"});
    }

    @Test
    public void toString1()
    {
        SqlFlow sqlFlow = SqlFlow.of(query.getBytes(UTF_8));
        Assert.assertEquals(sqlFlow.toString(), query);
    }
}
