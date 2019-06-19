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
package ideal.sylph.spi.utils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class JsonTextUtilTest
{
    @Test
    public void readJsonText()
    {
        String jsonText = "{} //comment...";
        String json = JsonTextUtil.readJsonText(jsonText);
        Assert.assertEquals(json.trim(), "{}");
    }

    @Test
    public void readJsonText2()
    {
        String jsonText = "{\"k1\":\"v1 // \"} //comment...";
        String json = JsonTextUtil.readJsonText(jsonText);
        Assert.assertEquals(json.trim(), "{\"k1\":\"v1 // \"}");
    }

    @Test
    public void readJsonText3()
    {
        String jsonText = "\n{\n\n} //comment...";
        String json = JsonTextUtil.readJsonText(jsonText);
        Assert.assertEquals(json.trim(), "{\n}");
    }
}