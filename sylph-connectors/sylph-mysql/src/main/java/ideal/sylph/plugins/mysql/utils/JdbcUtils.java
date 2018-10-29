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
package ideal.sylph.plugins.mysql.utils;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JdbcUtils
{
    private JdbcUtils() {}

    /**
     * jdbc ResultSet to List<Map></>
     *
     * @param rs input jdbc ResultSet
     * @return List<Map>
     */
    public static List<Map<String, Object>> resultToList(ResultSet rs)
            throws SQLException
    {
        ImmutableList.Builder<Map<String, Object>> listBuilder = ImmutableList.builder();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.builder();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                Object value = rs.getObject(i);
                if (value != null) {
                    mapBuilder.put(columnName, value);
                }
            }
            listBuilder.add(mapBuilder.build());
        }
        return listBuilder.build();
    }
}
