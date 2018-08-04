package ideal.sylph.parser.tree;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class Node
{
    private final Optional<NodeLocation> location;

    protected Node(Optional<NodeLocation> location)
    {
        this.location = requireNonNull(location, "location is null");
    }

    /**
     * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
     */
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNode(this, context);
    }

    public Optional<NodeLocation> getLocation()
    {
        return location;
    }

    public abstract List<? extends Node> getChildren();

    // Force subclasses to have a proper equals and hashcode implementation
    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();
}
