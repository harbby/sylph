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
package ideal.sylph.plugins.mysql;

import com.github.harbby.gadtry.ioc.IocFactory;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.PluginConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MysqlSinkTest
{
    @Test
    public void createMysqlConfig()
            throws Exception
    {
        String query = "select 1";
        Map<String, Object> config = ImmutableMap.<String, Object>builder()
                .put("query", query)
                .build();
        MysqlSink.MysqlConfig mysqlConfig =
                PluginConfigFactory.INSTANCE.createPluginConfig(MysqlSink.MysqlConfig.class, config);
        Assert.assertEquals(mysqlConfig.getQuery(), query);
    }

    @Test
    public void createMysqlSink()
    {
        IocFactory iocFactory = IocFactory.create();
        Map<String, Object> config = ImmutableMap.<String, Object>builder()
                .put("query", "insert into table1 values(${0},${1},${2})")
                .build();
        MysqlSink sink = NodeLoader.getPluginInstance(MysqlSink.class, iocFactory, config);
        Assert.assertNotNull(sink);
    }
}