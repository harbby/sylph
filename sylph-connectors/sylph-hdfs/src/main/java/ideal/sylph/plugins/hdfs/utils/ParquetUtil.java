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
package ideal.sylph.plugins.hdfs.utils;

import ideal.sylph.etl.Row;

import java.util.List;

public class ParquetUtil
{
    private ParquetUtil() {}

    /**
     * 构建Parquet的Schema
     *
     * @param fields 实际写入Parquet的字段集合
     * @return String 返回字符串
     */
    public static String buildSchema(List<Row.Field> fields)
    {
        StringBuilder sb = new StringBuilder("message row { ");

        for (Row.Field field : fields) {
            String fieldName = field.getName();
            Class<?> type = field.getJavaType();
            switch (type.getSimpleName()) {
                case "String":
                    sb.append("optional binary ").append(fieldName).append(" (UTF8); ");
                    break;
                case "Byte":
                case "Short":
                case "Integer":
                    sb.append("optional INT32 ").append(fieldName).append("; ");
                    break;
                case "Long":
                case "Date":
                    sb.append("optional INT64 ").append(fieldName).append("; ");
                    break;
                case "Float":
                    sb.append("optional FLOAT ").append(fieldName).append("; ");
                    break;
                case "Double":
                case "BigDecimal":
                    sb.append("optional DOUBLE ").append(fieldName).append("; ");
                    break;
                case "Boolean":
                    sb.append("optional BOOLEAN ").append(fieldName).append("; ");
                    break;
                case "byte[]":
                    sb.append("optional binary ").append(fieldName).append("; ");
                    break;
                case "Map":
                    throw new UnsupportedOperationException("this type[Map] have't support!");
                default:
                    sb.append("optional binary ").append(fieldName).append(" (UTF8); ");
                    break;
            }
        }
        sb.append("} ");
        return sb.toString();
    }
}
