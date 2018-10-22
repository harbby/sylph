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
package ideal.sylph.parser;

import com.google.common.collect.ImmutableList;
import ideal.sylph.parser.antlr4.SqlBaseBaseVisitor;
import ideal.sylph.parser.antlr4.SqlBaseLexer;
import ideal.sylph.parser.antlr4.SqlBaseParser;
import ideal.sylph.parser.tree.ColumnDefinition;
import ideal.sylph.parser.tree.CreateFunction;
import ideal.sylph.parser.tree.CreateStreamAsSelect;
import ideal.sylph.parser.tree.CreateTable;
import ideal.sylph.parser.tree.Expression;
import ideal.sylph.parser.tree.Identifier;
import ideal.sylph.parser.tree.InsertInto;
import ideal.sylph.parser.tree.IntervalLiteral;
import ideal.sylph.parser.tree.Node;
import ideal.sylph.parser.tree.NodeLocation;
import ideal.sylph.parser.tree.Property;
import ideal.sylph.parser.tree.QualifiedName;
import ideal.sylph.parser.tree.SelectQuery;
import ideal.sylph.parser.tree.StringLiteral;
import ideal.sylph.parser.tree.TableElement;
import ideal.sylph.parser.tree.WaterMark;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AstBuilder
        extends SqlBaseBaseVisitor<Node>
{
    @Override
    public Node visitProperty(SqlBaseParser.PropertyContext context)
    {
        return new Property(getLocation(context), (Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitBasicStringLiteral(SqlBaseParser.BasicStringLiteralContext context)
    {
        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
    }

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context)
    {
        return visit(context.statement());
    }

    @Override
    public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context)
    {
        //create table foo(name varchar)
        return new Identifier(getLocation(context), context.getText(), false);
    }

    @Override
    public Node visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext context)
    {
        //create table foo("name" varchar)
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("\"\"", "\"");

        return new Identifier(getLocation(context), identifier, true);
    }

    @Override
    public Node visitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext context)
    {
        //create table foo(`name` varchar)
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("\"\"", "\"");

        return new Identifier(getLocation(context), identifier, true);
    }

    @Override
    public Node visitCreateFunction(SqlBaseParser.CreateFunctionContext context)
    {
        Identifier functionName = visit(context.identifier(), Identifier.class);
        StringLiteral classString = visit(context.string(), StringLiteral.class);

        return new CreateFunction(getLocation(context), functionName, classString);
    }

    @Override
    public Node visitCreateStreamAsSelect(SqlBaseParser.CreateStreamAsSelectContext context)
    {
        Optional<String> comment = Optional.empty();
        // 词法分析后 获取原始输入字符串
        SqlBaseParser.QueryStreamContext queryContext = context.queryStream();
        int a = queryContext.start.getStartIndex();
        int b = queryContext.stop.getStopIndex();
        Interval interval = new Interval(a, b);
        String viewSql = context.start.getInputStream().getText(interval);

        return new CreateStreamAsSelect(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                context.EXISTS() != null,
                comment,
                visitIfPresent(context.watermark(), WaterMark.class),
                viewSql
        );
    }

    @Override
    public Node visitWatermark(SqlBaseParser.WatermarkContext context)
    {
        List<Identifier> field = visit(context.identifier(), Identifier.class);
        if (context.SYSTEM_OFFSET() != null) {
            int offset = Integer.parseInt(context.offset.getText());
            return new WaterMark(getLocation(context), field, new WaterMark.SystemOffset(offset));
        }
        else if (context.ROWMAX_OFFSET() != null) {
            int offset = Integer.parseInt(context.offset.getText());
            return new WaterMark(getLocation(context), field, new WaterMark.RowMaxOffset(offset));
        }
        else {
            throw new IllegalArgumentException("Unable to determine Watermark type: " + context.getText());
        }
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }
        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().property(), Property.class);
        }

        CreateTable.Type type = null;
        if (context.SINK() != null || context.OUTPUT() != null) {
            type = CreateTable.Type.SINK;
        }
        else if (context.SOURCE() != null || context.INPUT() != null) {
            type = CreateTable.Type.SOURCE;
        }
        else if (context.BATCH() != null) {
            type = CreateTable.Type.BATCH;
        }

        return new CreateTable(
                requireNonNull(type, "table type is null,but must is SOURCE or SINK or BATCH"),
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                visit(context.tableElement(), TableElement.class),
                context.EXISTS() != null,
                properties,
                comment,
                visitIfPresent(context.watermark(), WaterMark.class));
    }

    @Override
    public Node visitInsertInto(SqlBaseParser.InsertIntoContext context)
    {
        String insert = getNodeText(context);

        return new InsertInto(getLocation(context), insert);
    }

    @Override
    public Node visitSelectQuery(SqlBaseParser.SelectQueryContext context)
    {
        String query = getNodeText(context);
        return new SelectQuery(getLocation(context), query);
    }

    private static String getNodeText(ParserRuleContext context)
    {
        int a = context.start.getStartIndex();
        int b = context.stop.getStopIndex();
        Interval interval = new Interval(a, b);
        String text = context.start.getInputStream().getText(interval);
        return text;
    }

    @Override
    public Node visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }
        return new ColumnDefinition(
                getLocation(context),
                (Identifier) visit(context.identifier()),
                getType(context.type()),
                comment);
    }

    private String getType(SqlBaseParser.TypeContext type)
    {
        if (type.baseType() != null) {
            String signature = type.baseType().getText();
            if (type.baseType().DOUBLE_PRECISION() != null) {
                // TODO: Temporary hack that should be removed with new planner.
                signature = "DOUBLE";
            }
            if (!type.typeParameter().isEmpty()) {
                String typeParameterSignature = type
                        .typeParameter()
                        .stream()
                        .map(this::typeParameterToString)
                        .collect(Collectors.joining(","));
                signature += "(" + typeParameterSignature + ")";
            }
            return signature;
        }

        if (type.ARRAY() != null) {
            return "ARRAY(" + getType(type.type(0)) + ")";
        }

        if (type.MAP() != null) {
            return "MAP(" + getType(type.type(0)) + "," + getType(type.type(1)) + ")";
        }

        if (type.ROW() != null) {
            StringBuilder builder = new StringBuilder("(");
            for (int i = 0; i < type.identifier().size(); i++) {
                if (i != 0) {
                    builder.append(",");
                }
                builder.append(visit(type.identifier(i)))
                        .append(" ")
                        .append(getType(type.type(i)));
            }
            builder.append(")");
            return "ROW" + builder.toString();
        }

        throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
    }

    private String typeParameterToString(SqlBaseParser.TypeParameterContext typeParameter)
    {
        if (typeParameter.INTEGER_VALUE() != null) {
            return typeParameter.INTEGER_VALUE().toString();
        }
        if (typeParameter.type() != null) {
            return getType(typeParameter.type());
        }
        throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
    }

    private static String unquote(String value)
    {
        return value.substring(1, value.length() - 1)
                .replace("''", "'");
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz)
    {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz)
    {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    private <T> T visit(ParserRuleContext context, Class<T> clazz)
    {
        return clazz.cast(visit(context));
    }

    public static NodeLocation getLocation(ParserRuleContext parserRuleContext)
    {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(Token token)
    {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }

    private QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context)
    {
        List<String> parts = visit(context.identifier(), Identifier.class).stream()
                .map(Identifier::getValue) // TODO: preserve quotedness
                .collect(Collectors.toList());

        return QualifiedName.of(parts);
    }

    private static IntervalLiteral.IntervalField getIntervalFieldType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.YEAR:
                return IntervalLiteral.IntervalField.YEAR;
            case SqlBaseLexer.MONTH:
                return IntervalLiteral.IntervalField.MONTH;
            case SqlBaseLexer.DAY:
                return IntervalLiteral.IntervalField.DAY;
            case SqlBaseLexer.HOUR:
                return IntervalLiteral.IntervalField.HOUR;
            case SqlBaseLexer.MINUTE:
                return IntervalLiteral.IntervalField.MINUTE;
            case SqlBaseLexer.SECOND:
                return IntervalLiteral.IntervalField.SECOND;
        }

        throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
    }

    private static IntervalLiteral.Sign getIntervalSign(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.MINUS:
                return IntervalLiteral.Sign.NEGATIVE;
            case SqlBaseLexer.PLUS:
                return IntervalLiteral.Sign.POSITIVE;
        }

        throw new IllegalArgumentException("Unsupported sign: " + token.getText());
    }
}
