package ideal.sylph.parser;

import com.google.common.collect.ImmutableList;
import ideal.sylph.parser.antlr4.SqlBaseBaseVisitor;
import ideal.sylph.parser.antlr4.SqlBaseLexer;
import ideal.sylph.parser.antlr4.SqlBaseParser;
import ideal.sylph.parser.tree.ColumnDefinition;
import ideal.sylph.parser.tree.CreateTable;
import ideal.sylph.parser.tree.Expression;
import ideal.sylph.parser.tree.Identifier;
import ideal.sylph.parser.tree.IntervalLiteral;
import ideal.sylph.parser.tree.Node;
import ideal.sylph.parser.tree.NodeLocation;
import ideal.sylph.parser.tree.Property;
import ideal.sylph.parser.tree.QualifiedName;
import ideal.sylph.parser.tree.StringLiteral;
import ideal.sylph.parser.tree.TableElement;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

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
        return new Identifier(getLocation(context), context.getText(), false);
    }

    @Override
    public Node visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext context)
    {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("\"\"", "\"");

        return new Identifier(getLocation(context), identifier, true);
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
        return new CreateTable(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                visit(context.tableElement(), TableElement.class),
                context.EXISTS() != null,
                properties,
                comment);
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

    @Override
    public Node visitInterval(SqlBaseParser.IntervalContext context)
    {
        return new IntervalLiteral(
                getLocation(context),
                ((StringLiteral) visit(context.string())).getValue(),
                Optional.ofNullable(context.sign)
                        .map(AstBuilder::getIntervalSign)
                        .orElse(IntervalLiteral.Sign.POSITIVE),
                getIntervalFieldType((Token) context.from.getChild(0).getPayload()),
                Optional.ofNullable(context.to)
                        .map((x) -> x.getChild(0).getPayload())
                        .map(Token.class::cast)
                        .map(AstBuilder::getIntervalFieldType));
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

        if (type.INTERVAL() != null) {
            return "INTERVAL " + getIntervalFieldType((Token) type.from.getChild(0).getPayload()) +
                    " TO " + getIntervalFieldType((Token) type.to.getChild(0).getPayload());
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

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz)
    {
//        for(ParserRuleContext context : contexts){
//            Node node = this.visit(context);
//            if(clazz == TableElement.class){
//                System.out.println(clazz);
//            }
//            T t = clazz.cast(node);
//            System.out.println(t);
//        }
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
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
