package ideal.sylph.parser;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import ideal.sylph.parser.tree.AstVisitor;
import ideal.sylph.parser.tree.Expression;
import ideal.sylph.parser.tree.Identifier;
import ideal.sylph.parser.tree.Node;
import ideal.sylph.parser.tree.StringLiteral;

import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class ExpressionFormatter
{
    private ExpressionFormatter() {}

    public static String formatExpression(Expression expression, Optional<List<Expression>> parameters)
    {
        return new Formatter(parameters).process(expression, null);
    }

    public static class Formatter
            extends AstVisitor<String, Void>
    {
        private final Optional<List<Expression>> parameters;

        public Formatter(Optional<List<Expression>> parameters)
        {
            this.parameters = parameters;
        }

        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

//        @Override
//        protected String visitBooleanLiteral(BooleanLiteral node, Void context)
//        {
//            return String.valueOf(node.getValue());
//        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return formatStringLiteral(node.getValue());
        }

//        @Override
//        protected String visitCharLiteral(CharLiteral node, Void context)
//        {
//            return "CHAR " + formatStringLiteral(node.getValue());
//        }

        @Override
        protected String visitIdentifier(Identifier node, Void context)
        {
            if (!node.isDelimited()) {
                return node.getValue();
            }
            else {
                return '"' + node.getValue().replace("\"", "\"\"") + '"';
            }
        }

        static String formatStringLiteral(String s)
        {
            s = s.replace("'", "''");
            if (CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(s)) {
                return "'" + s + "'";
            }

            StringBuilder builder = new StringBuilder();
            builder.append("U&'");
            PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
            while (iterator.hasNext()) {
                int codePoint = iterator.nextInt();
                checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
                if (isAsciiPrintable(codePoint)) {
                    char ch = (char) codePoint;
                    if (ch == '\\') {
                        builder.append(ch);
                    }
                    builder.append(ch);
                }
                else if (codePoint <= 0xFFFF) {
                    builder.append('\\');
                    builder.append(String.format("%04X", codePoint));
                }
                else {
                    builder.append("\\+");
                    builder.append(String.format("%06X", codePoint));
                }
            }
            builder.append("'");
            return builder.toString();
        }

        private static boolean isAsciiPrintable(int codePoint)
        {
            if (codePoint >= 0x7F || codePoint < 0x20) {
                return false;
            }
            return true;
        }

        private String visitFilter(Expression node, Void context)
        {
            return "(WHERE " + process(node, context) + ')';
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right)
        {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }

        private String joinExpressions(List<Expression> expressions)
        {
            return Joiner.on(", ").join(expressions.stream()
                    .map((e) -> process(e, null))
                    .iterator());
        }
    }
}
