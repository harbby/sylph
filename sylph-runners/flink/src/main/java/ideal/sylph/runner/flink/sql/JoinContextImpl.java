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
package ideal.sylph.runner.flink.sql;

import ideal.sylph.etl.Row;
import ideal.sylph.etl.join.JoinContext;
import ideal.sylph.etl.join.SelectField;
import ideal.sylph.parser.calcite.JoinInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class JoinContextImpl
        implements JoinContext
{
    private final String batchTable;
    private final JoinType joinType;
    private final List<SelectField> selectFields;
    private final Map<Integer, String> joinOnMapping;

    private JoinContextImpl(String batchTable, JoinType joinType, List<SelectField> selectFields, Map<Integer, String> joinOnMapping)
    {
        this.batchTable = batchTable;
        this.joinType = joinType;
        this.selectFields = selectFields;

        this.joinOnMapping = joinOnMapping;
    }

    @Override
    public String getBatchTable()
    {
        return batchTable;
    }

    @Override
    public JoinType getJoinType()
    {
        return joinType;
    }

    @Override
    public List<SelectField> getSelectFields()
    {
        return selectFields;
    }

    @Override
    public Map<Integer, String> getJoinOnMapping()
    {
        return joinOnMapping;
    }

    @Override
    public Row.Schema getSchema()
    {
        return Row.Schema.newBuilder().build();
    }

    public static JoinContext createContext(JoinInfo joinInfo, RowTypeInfo streamRowType, List<SelectField> joinSelectFields)
    {
        JoinContext.JoinType joinType = transJoinType(joinInfo.getJoinType());

        Map<Integer, String> joinOnMapping = joinInfo.getJoinOnMapping()
                .entrySet().stream()
                .collect(Collectors.toMap(k -> {
                    int streamFieldIndex = streamRowType.getFieldIndex(k.getKey());
                    checkState(streamFieldIndex != -1, "can't deal equal field: " + k.getKey());
                    return streamFieldIndex;
                }, Map.Entry::getValue));

        return new JoinContextImpl(joinInfo.getBatchTable().getName(), joinType, joinSelectFields, joinOnMapping);
    }

    private static JoinContext.JoinType transJoinType(org.apache.calcite.sql.JoinType joinType)
    {
        switch (joinType) {
            case INNER:
                return JoinContext.JoinType.INNER;
            case FULL:
                return JoinContext.JoinType.FULL;
            case CROSS:
                return JoinContext.JoinType.CROSS;
            case LEFT:
                return JoinContext.JoinType.LEFT;
            case RIGHT:
                return JoinContext.JoinType.RIGHT;
            default:
                throw new IllegalArgumentException("this " + joinType + " have't support!");
        }
    }
}
