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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.harbby.gadtry.base.Strings;

import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SqlFlow
        extends Flow
{
    /*
     *   use regex split sqlText
     *
     *  '  ---->        ;(?=([^']*'[^']*')*[^']*$)
     *  ' and "" ---->  ;(?=([^']*'[^']*')*[^']*$)(?=([^"]*"[^"]*")*[^"]*$)
     * */
    public static final String SQL_REGEX = ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)(?=([^']*'[^']*')*[^']*$)";
    private final String[] sqlSplit;
    private final String sqlText;

    public SqlFlow(String sqlText)
    {
        this.sqlText = sqlText;
        this.sqlSplit = Stream.of(sqlText.split(SQL_REGEX))
                .filter(Strings::isNotBlank).toArray(String[]::new);
    }

    public SqlFlow(byte[] flowBytes)
    {
        this(new String(flowBytes, UTF_8));
    }

    public static SqlFlow of(byte[] flowBytes)
    {
        return new SqlFlow(flowBytes);
    }

    @JsonIgnore
    public String[] getSqlSplit()
    {
        return sqlSplit;
    }

    @Override
    public String toString()
    {
        return sqlText;
    }
}
