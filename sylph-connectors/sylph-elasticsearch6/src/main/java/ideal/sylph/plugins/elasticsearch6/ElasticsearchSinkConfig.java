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
package ideal.sylph.plugins.elasticsearch6;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;

public class ElasticsearchSinkConfig
        extends PluginConfig
{
    @Name("cluster_name")
    @Description("this is es cluster name")
    private String clusterName;

    @Name("cluster_hosts")
    @Description("this is es cluster hosts")
    private String hosts;

    @Name("es_index")
    @Description("this is es index")
    private String index;

    @Name("id_field")
    @Description("this is es id_field")
    private String idField;

    @Name("update")
    @Description("update or insert")
    private boolean update = false;

    @Name("batchSize")
    @Description("this write batch size")
    private int batchSize = 1024;

    @Name("index_type")
    @Description("this is es index_type, Do not set")
    private String type = "default";

    public int getBatchSize()
    {
        return batchSize;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public String getHosts()
    {
        return hosts;
    }

    public String getIndex()
    {
        return index;
    }

    public String getIdField()
    {
        return idField;
    }

    public boolean isUpdate()
    {
        return update;
    }

    public String getType()
    {
        return type;
    }
}
