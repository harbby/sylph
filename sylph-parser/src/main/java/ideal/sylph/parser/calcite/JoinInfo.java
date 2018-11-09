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
package ideal.sylph.parser.calcite;

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.Map;

/**
 * stream join batch join Info
 */
public class JoinInfo
{
    private final TableName leftTable;
    private final TableName rightTable;
    private final JoinType joinType;
    private final boolean leftIsBatch;
    private final boolean rightIsBatch;
    private final SqlJoin sqlJoin;

    private SqlNode joinWhere;  //谓词需要 join前先进行pushDown
    private SqlSelect joinSelect;  //join 所在的Select
    private Map<String, String> joinOnMapping;

    JoinInfo(SqlJoin sqlJoin, TableName leftTable, TableName rightTable, boolean leftIsBatch, boolean rightIsBatch)
    {
        this.sqlJoin = sqlJoin;
        this.leftIsBatch = leftIsBatch;
        this.rightIsBatch = rightIsBatch;
        this.joinType = sqlJoin.getJoinType();
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }

    public SqlJoin getSqlJoin()
    {
        return sqlJoin;
    }

    public boolean getLeftIsBatch()
    {
        return leftIsBatch;
    }

    public boolean getRightIsBatch()
    {
        return rightIsBatch;
    }

    public SqlNode getLeftNode()
    {
        return sqlJoin.getLeft();
    }

    public SqlNode getRightNode()
    {
        return sqlJoin.getRight();
    }

    public TableName getLeftTable()
    {
        return leftTable;
    }

    public TableName getRightTable()
    {
        return rightTable;
    }

    public void setJoinSelect(SqlSelect sqlSelect)
    {
        this.joinSelect = sqlSelect;
    }

    public SqlSelect getJoinSelect()
    {
        return joinSelect;
    }

    public void setJoinOnMapping(Map<String, String> joinOnMapping)
    {
        this.joinOnMapping = joinOnMapping;
    }

    public Map<String, String> getJoinOnMapping()
    {
        return joinOnMapping;
    }

    public TableName getBatchTable()
    {
        return getRightIsBatch() ? rightTable : leftTable;
    }

    public TableName getStreamTable()
    {
        return getRightIsBatch() ? leftTable : rightTable;
    }

    public JoinType getJoinType()
    {
        return joinType;
    }

    /**
     * get join out tmp table name
     */
    public String getJoinTableName()
    {
        return this.getLeftTable().getAliasOrElseName() + "_" + this.getRightTable().getAliasOrElseName();
    }
}
