package ideal.sylph.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Property
        extends Node
{
    private final Identifier name;
    private final Expression value;

    public Property(Identifier name, Expression value)
    {
        this(Optional.empty(), name, value);
    }

    public Property(NodeLocation location, Identifier name, Expression value)
    {
        this(Optional.of(location), name, value);
    }

    private Property(Optional<NodeLocation> location, Identifier name, Expression value)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.value = requireNonNull(value, "value is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public Expression getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitProperty(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(name, value);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Property other = (Property) obj;
        return Objects.equals(name, other.name) &&
                Objects.equals(value, other.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("value", value)
                .toString();
    }
}
