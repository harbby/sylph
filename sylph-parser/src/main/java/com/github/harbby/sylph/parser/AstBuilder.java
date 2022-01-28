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
package com.github.harbby.sylph.parser;

import com.github.harbby.sylph.parser.antlr4.SqlBaseBaseVisitor;
import com.github.harbby.sylph.parser.antlr4.SqlBaseParser;
import com.github.harbby.sylph.parser.tree.AllowedLateness;
import com.github.harbby.sylph.parser.tree.BooleanLiteral;
import com.github.harbby.sylph.parser.tree.ColumnDefinition;
import com.github.harbby.sylph.parser.tree.CreateFunction;
import com.github.harbby.sylph.parser.tree.CreateStreamAsSelect;
import com.github.harbby.sylph.parser.tree.CreateTable;
import com.github.harbby.sylph.parser.tree.DoubleLiteral;
import com.github.harbby.sylph.parser.tree.Expression;
import com.github.harbby.sylph.parser.tree.Identifier;
import com.github.harbby.sylph.parser.tree.InsertInto;
import com.github.harbby.sylph.parser.tree.LongLiteral;
import com.github.harbby.sylph.parser.tree.Node;
import com.github.harbby.sylph.parser.tree.NodeLocation;
import com.github.harbby.sylph.parser.tree.Proctime;
import com.github.harbby.sylph.parser.tree.Property;
import com.github.harbby.sylph.parser.tree.QualifiedName;
import com.github.harbby.sylph.parser.tree.SelectQuery;
import com.github.harbby.sylph.parser.tree.StringLiteral;
import com.github.harbby.sylph.parser.tree.TableElement;
import com.github.harbby.sylph.parser.tree.WaterMark;
import com.github.harbby.sylph.parser.tree.WindowTrigger;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AstBuilder
        extends SqlBaseBaseVisitor<Node>
{
    @Override
    public Node visitProperty(SqlBaseParser.PropertyContext context)
    {
        String withKey = visit(context.qualifiedName().identifier(), Identifier.class).stream()
                .map(Identifier::getValue)
                .collect(Collectors.joining("."));

        return new Property(getLocation(context), withKey, (Expression) visit(context.expression()));
    }

    @Override
    public Node visitBooleanValue(SqlBaseParser.BooleanValueContext context)
    {
        return new BooleanLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext context)
    {
        return new DoubleLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context)
    {
        return new DoubleLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context)
    {
        return new LongLiteral(getLocation(context), context.getText());
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
        SqlBaseParser.QueryStreamContext queryContext = context.queryStream();
        int a = queryContext.start.getStartIndex();
        int b = queryContext.stop.getStopIndex();
        Interval interval = new Interval(a, b);
        String viewSql = context.start.getInputStream().getText(interval);

        return new CreateStreamAsSelect(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                context.EXISTS() != null,
                visitIfPresent(context.watermark(), WaterMark.class).orElse(null),
                viewSql);
    }

    @Override
    public Node visitTrigger(SqlBaseParser.TriggerContext ctx)
    {
        SqlBaseParser.StringContext stringContext = ctx.string();
        if (stringContext != null) {
            return new WindowTrigger(getLocation(ctx), visit(stringContext, StringLiteral.class).getValue());
        }
        else {
            return new WindowTrigger(getLocation(ctx), ctx.value.getText());
        }
    }

    @Override
    public Node visitAllowedLateness(SqlBaseParser.AllowedLatenessContext ctx)
    {
        SqlBaseParser.StringContext stringContext = ctx.string();
        if (stringContext != null) {
            return new AllowedLateness(getLocation(ctx), visit(stringContext, StringLiteral.class).getValue());
        }
        else {
            return new AllowedLateness(getLocation(ctx), ctx.value.getText());
        }
    }

    @Override
    public Node visitWatermark(SqlBaseParser.WatermarkContext context)
    {
        List<Identifier> fields = visit(context.identifier(), Identifier.class);
        checkState(fields.size() == 2, "WATERMARK FOR rowtime AS event_time - INTERVAL ...");
        long offset = Long.parseLong(visit(context.string(), StringLiteral.class).getValue());
        if (context.SECOND() != null) {
            offset = TimeUnit.SECONDS.toMillis(offset);
        }
        else if (context.MINUTE() != null) {
            offset = TimeUnit.MINUTES.toMillis(offset);
        }
        return new WaterMark(getLocation(context), fields.get(0), fields.get(1), offset);
    }

    @Override
    public Node visitProctime(SqlBaseParser.ProctimeContext context)
    {
        return new Proctime(getLocation(context), (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext context)
    {
        String comment = null;
        if (context.COMMENT() != null) {
            comment = ((StringLiteral) visit(context.string())).getValue();
        }
        List<Property> properties = Collections.emptyList();
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

        List<TableElement> elements = visit(context.tableElement(), TableElement.class);
        return new CreateTable(
                requireNonNull(type, "table type is null,but must is SOURCE or SINK or BATCH"),
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                elements.stream().filter(x -> x instanceof ColumnDefinition).map(ColumnDefinition.class::cast).collect(toList()),
                elements.stream().filter(x -> x instanceof Proctime).map(Proctime.class::cast).collect(toList()),
                context.EXISTS() != null,
                properties,
                comment,
                visitIfPresent(context.watermark(), WaterMark.class).orElse(null));
    }

    @Override
    public Node visitInsertInto(SqlBaseParser.InsertIntoContext context)
    {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        SelectQuery selectQuery = context.withQuery() == null ?
                visit(context.queryStream(), SelectQuery.class) :
                visit(context.withQuery(), SelectQuery.class);

        Interval interval = new Interval(context.start.getStartIndex(), selectQuery.getQueryEndIndex());
        String insert = context.start.getInputStream().getText(interval);
        //String insert = getNodeText(context);

        return new InsertInto(getLocation(context), insert, qualifiedName, selectQuery);
    }

    @Override
    public Node visitQueryStream(SqlBaseParser.QueryStreamContext ctx)
    {
        SqlBaseParser.AllowedLatenessContext allowedLatenessContext = ctx.allowedLateness();
        SqlBaseParser.TriggerContext triggerContext = ctx.trigger();

        AllowedLateness allowedLateness = null;
        WindowTrigger windowTrigger = null;

        Stream.Builder<Integer> builder = Stream.builder();
        if (allowedLatenessContext != null) {
            builder.add(allowedLatenessContext.start.getStartIndex() - 1);
            allowedLateness = visit(allowedLatenessContext, AllowedLateness.class);
        }
        if (triggerContext != null) {
            builder.add(triggerContext.start.getStartIndex() - 1);
            windowTrigger = visit(triggerContext, WindowTrigger.class);
        }

        int queryEnd = builder.build().reduce((x, y) -> x < y ? x : y).orElse(ctx.stop.getStopIndex());
        Interval interval = new Interval(ctx.start.getStartIndex(), queryEnd);
        String query = ctx.start.getInputStream().getText(interval);

        //String fullQuery = getNodeText(context);
        return new SelectQuery(getLocation(ctx), query, queryEnd, allowedLateness, windowTrigger);
    }

    @Override
    public Node visitSelectQuery(SqlBaseParser.SelectQueryContext context)
    {
        return visit(context.queryStream(), SelectQuery.class);
    }

    @Override
    public Node visitWithQuery(SqlBaseParser.WithQueryContext ctx)
    {
        Map<Identifier, SelectQuery> withTableQuery = new LinkedHashMap<>();
        for (SqlBaseParser.AsQueryContext asQueryContext : ctx.asQuery()) {
            SelectQuery selectQuery = visit(asQueryContext.queryStream(), SelectQuery.class);
            Identifier identifier = visit(asQueryContext.identifier(), Identifier.class);
            withTableQuery.put(identifier, selectQuery);
        }
        SelectQuery selectQuery = visit(ctx.queryStream(), SelectQuery.class);
        selectQuery.setWithTableQuery(withTableQuery);

        return selectQuery;
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
    public Node visitExtend(SqlBaseParser.ExtendContext context)
    {
        return visit(context.string());
    }

    @Override
    public Node visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext context)
    {
        String comment = null;
        if (context.COMMENT() != null) {
            comment = ((StringLiteral) visit(context.string())).getValue();
        }
        String extend = null;
        if (context.extend() != null) {
            extend = ((StringLiteral) visit(context.extend())).getValue();
        }

        return new ColumnDefinition(
                getLocation(context),
                (Identifier) visit(context.identifier()),
                getType(context.type()),
                extend,
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
}
