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
package ideal.sylph.plugins.hbase.util;

import ideal.sylph.etl.Schema;
import ideal.sylph.plugins.hbase.HbaseSink;
import ideal.sylph.plugins.hbase.exception.ColumMappingException;
import ideal.sylph.plugins.hbase.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ColumUtil
{
    private ColumUtil() {}

    private static final String FAMILY_DEFAULT = "0";
    private static final Logger log = LoggerFactory.getLogger(HbaseSink.class);

    /**
     * HBase table field mapping, Inclusion column family and new column name.
     *
     * @param schema Table field definitions.
     * @param columnMappingStr Field information to be mapped.
     * @return Table field mapping result.
     */
    public static Map<String, Tuple2<String, String>> mapping(Schema schema, String columnMappingStr)
            throws Exception
    {
        Map<String, Tuple2<String, String>> columnMapping = new HashMap<>();
        schema.getFieldNames().forEach(fieldName -> columnMapping.put(fieldName, new Tuple2(FAMILY_DEFAULT, fieldName)));
        if (columnMappingStr != null && !"".equals(columnMappingStr)) {
            for (String columInfoStr : columnMappingStr.split(",")) {
                String[] columInfo = columInfoStr.split(":");
                switch (columInfo.length) {
                    case 2:
                        mappingTwoLength(columInfo, columnMapping);
                        break;
                    case 3:
                        mappingThreeLength(columInfo, columnMapping);
                        break;
                    default:
                        throw new ColumMappingException("Column mapping str is '" + columInfoStr + "', and Standard format is A:B:C or A:B .");
                }
            }
        }
        return columnMapping;
    }

    /**
     * Mapping format is A:B. A is hbase famliy and B is column name that is defined in the table field definitions.
     *
     * @param columInfo Field information Array.
     * @param columnMapping Table field mapping result.
     */
    private static void mappingTwoLength(String[] columInfo, Map<String, Tuple2<String, String>> columnMapping)
            throws Exception
    {
        String family = columInfo[0];
        String fieldName = columInfo[1];
        if (!columnMapping.containsKey(fieldName)) {
            throw new ColumMappingException("Table definitions do not contain field '" + fieldName + "'");
        }
        columnMapping.put(fieldName, new Tuple2<>(family, fieldName));
    }

    /**
     * Mapping format is A:B:C. A is original field name that is defined in the table field definitions, B is hbase famliy, C is new column name that stored in hbase table.
     *
     * @param columInfo Field information Array.
     * @param columnMapping Table field mapping result.
     */
    private static void mappingThreeLength(String[] columInfo, Map<String, Tuple2<String, String>> columnMapping)
            throws Exception
    {
        String originalName = columInfo[0];
        String family = columInfo[1];
        String mappingName = columInfo[2];
        if (!columnMapping.containsKey(originalName)) {
            throw new ColumMappingException("Table definitions do not contain field '" + originalName + "'");
        }
        log.warn("original cloumn name '" + originalName + "', new cloumn name '" + mappingName + "', hbase family '" + family + "'.");
        columnMapping.put(originalName, new Tuple2<>(family, mappingName));
    }
}
