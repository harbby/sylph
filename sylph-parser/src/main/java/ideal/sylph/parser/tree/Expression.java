package ideal.sylph.parser.tree;

import ideal.sylph.parser.ExpressionFormatter;

import java.util.Optional;

public abstract class Expression
        extends Node
{
    protected Expression(Optional<NodeLocation> location)
    {
        super(location);
    }

    /**
     * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
     */
    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }

    @Override
    public final String toString()
    {
        return ExpressionFormatter.formatExpression(this, Optional.empty()); // This will not replace parameters, but we don't have access to them here
    }
}
