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

import com.github.harbby.gadtry.collection.MutableList;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.JoinType.INNER;
import static org.apache.calcite.sql.JoinType.LEFT;
import static org.apache.calcite.sql.SqlKind.AS;
import static org.apache.calcite.sql.SqlKind.IDENTIFIER;
import static org.apache.calcite.sql.SqlKind.JOIN;
import static org.apache.calcite.sql.SqlKind.SELECT;
import static org.apache.calcite.sql.SqlKind.WITH;

public class CalciteSqlParser
{
    private final List<Object> plan = new ArrayList<>();
    private final Set<String> batchTables; //所有维度表 join的维度表一定要有

    private final SqlParser.Config sqlParserConfig = SqlParser
            .configBuilder()
            .setLex(Lex.JAVA)
            .build();

    public CalciteSqlParser(Set<String> batchTables)
    {
        this.batchTables = requireNonNull(batchTables, "batchTables is null");
    }

    public List<Object> getPlan(String joinSql)
            throws SqlParseException
    {
        return getPlan(joinSql, sqlParserConfig);
    }

    public List<Object> getPlan(String joinSql, SqlParser.Config sqlParserConfig)
            throws SqlParseException
    {
        SqlParser sqlParser = SqlParser.create(joinSql, sqlParserConfig);
        SqlNode sqlNode = sqlParser.parseStmt();

        SqlNode rootNode = sqlParse(sqlNode);
        plan.add(rootNode);
        return plan;
    }

    private SqlNode sqlParse(SqlNode sqlNode)
    {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case WITH: {
                SqlWith sqlWith = (SqlWith) sqlNode;
                SqlNodeList sqlNodeList = sqlWith.withList;
                for (SqlNode withAsTable : sqlNodeList) {
                    SqlWithItem sqlWithItem = (SqlWithItem) withAsTable;
                    sqlParse(sqlWithItem.query);
                    plan.add(sqlWithItem);
                }
                sqlParse(sqlWith.body);
                return sqlWith.body;
            }
            case SELECT: {
                SqlNode sqlFrom = ((SqlSelect) sqlNode).getFrom();
                if (sqlFrom == null) {
                    return sqlNode;
                }
                if (sqlFrom.getKind() == IDENTIFIER) {
                    String tableName = ((SqlIdentifier) sqlFrom).getSimple();
                    checkState(!batchTables.contains(tableName), "维度表不能直接用来 from");
                }
                else if (sqlFrom.getKind() == AS) {
                    TableName tableName = parserAs((SqlBasicCall) sqlFrom);
                    checkState(!batchTables.contains(tableName.getName()), "维度表不能直接用来 from");
                }
                else if (sqlFrom.getKind() == JOIN) {
                    JoinInfo result = parserJoin((SqlJoin) sqlFrom);
                    buildJoinQuery(result, (SqlSelect) sqlNode);
                }
                return sqlNode;
            }
            case INSERT:
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                //break;
                SqlSelect result = (SqlSelect) sqlParse(sqlSource);
                ((SqlInsert) sqlNode).setSource(result);
                return sqlNode;
            default:
                //throw new UnsupportedOperationException(sqlNode.toString());
                return sqlNode;
        }
    }

    private TableName parserAs(SqlBasicCall sqlNode)
    {
        SqlNode query = sqlNode.getOperands()[0];
        SqlNode alias = sqlNode.getOperands()[1];
        String tableName = "";
        if (query.getKind() == IDENTIFIER) {
            tableName = query.toString();
        }
        else {  //is query 子查询
            sqlParse(query);  //parser 子查询？
        }

        return new TableName(tableName, Optional.ofNullable(alias.toString()));
    }

    private void buildJoinQuery(JoinInfo joinInfo, SqlSelect sqlSelect)
    {
        if (joinInfo.getLeftIsBatch() == joinInfo.getRightIsBatch()) {
            return;
        }
        checkState(joinInfo.getJoinType() == INNER || joinInfo.getJoinType() == LEFT, "Sorry, we currently only support left join and inner join. but your " + joinInfo.getJoinType());

        //next stream join batch
        joinInfo.setJoinSelect(sqlSelect);

        SqlNode streamNode = joinInfo.getRightIsBatch() ? joinInfo.getLeftNode() : joinInfo.getRightNode();
        if (streamNode.getKind() == AS) {  //如果是子查询 则对子查询再进一步进行解析
            SqlNode query = ((SqlBasicCall) streamNode).operand(0);
            if (query.getKind() == SELECT || query.getKind() == WITH) {
                plan.add(streamNode);
            }
        }
        else if (streamNode.getKind() == SELECT) {
            throw new IllegalArgumentException("Select sub query must have `as` an alias");
        }
        plan.add(joinInfo);

        SqlNode joinOn = joinInfo.getSqlJoin().getCondition();
        List<SqlNode> sqlNodeList = joinOn.getKind() == SqlKind.AND
                ? MutableList.of(((SqlBasicCall) joinOn).getOperands())
                : MutableList.of(joinOn);

        /*
         * joinOnMapping is Map<streamField,batchField>
         * */
        final Map<String, String> joinOnMapping = sqlNodeList.stream()
                .map(sqlNode -> {
                    checkState(sqlNode.getKind() == SqlKind.EQUALS, "Only support EQUALS join on !");
                    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                    SqlIdentifier leftField = sqlBasicCall.operand(0);
                    SqlIdentifier rightField = sqlBasicCall.operand(1);
                    checkState(!leftField.isStar() && !rightField.isStar(), "join on not support table.*");
                    return sqlBasicCall;
                }).map(sqlBasicCall -> {
                    SqlIdentifier onLeft = sqlBasicCall.operand(0);
                    SqlIdentifier onRight = sqlBasicCall.operand(1);

                    String leftTableName = onLeft.getComponent(0).getSimple();
                    String leftField = onLeft.getComponent(1).getSimple();
                    String rightTableName = onRight.getComponent(0).getSimple();
                    String rightField = onRight.getComponent(1).getSimple();

                    if (leftTableName.equalsIgnoreCase(joinInfo.getBatchTable().getAliasOrElseName())) {
                        return new String[] {rightField, leftField};
                    }
                    else if (rightTableName.equalsIgnoreCase(joinInfo.getBatchTable().getAliasOrElseName())) {
                        return new String[] {leftField, rightField};
                    }
                    else {
                        throw new IllegalArgumentException("无batchBable 字段进行join on" + sqlBasicCall);
                    }
                }).collect(Collectors.toMap(k -> k[0], v -> v[1]));
        joinInfo.setJoinOnMapping(joinOnMapping);

        //Update from node
        String joinOutTableName = joinInfo.getJoinTableName();
        SqlParserPos sqlParserPos = new SqlParserPos(0, 0);

        SqlIdentifier sqlIdentifier = new SqlIdentifier(joinOutTableName, sqlParserPos);
        sqlSelect.setFrom(sqlIdentifier);
    }

    private JoinInfo parserJoin(SqlJoin sqlJoin)
    {
        final Function<SqlNode, TableName> func = (node) -> {
            TableName tableName;
            if (node.getKind() == IDENTIFIER) {
                String leftTableName = node.toString();
                tableName = new TableName(leftTableName, Optional.empty());
            }
            else if (node.getKind() == JOIN) {
                JoinInfo nodeJoinInfo = parserJoin((SqlJoin) node);
                throw new UnsupportedOperationException("this have't support!");
            }
            else if (node.getKind() == AS) {
                tableName = parserAs((SqlBasicCall) node);
            }
            else {
                throw new UnsupportedOperationException("this have't support! " + node);
            }
            return tableName;
        };

        TableName leftTable = func.apply(sqlJoin.getLeft());
        TableName rightTable = func.apply(sqlJoin.getRight());

        return new JoinInfo(sqlJoin, leftTable, rightTable, batchTables.contains(leftTable.getName()), batchTables.contains(rightTable.getName()));
    }
}
