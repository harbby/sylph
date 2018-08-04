package ideal.sylph.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ColumnDefinition
        extends TableElement
{
    private final Identifier name;
    private final String type;
    private final Optional<String> comment;

    public ColumnDefinition(Identifier name, String type, Optional<String> comment)
    {
        this(Optional.empty(), name, type, comment);
    }

    public ColumnDefinition(NodeLocation location, Identifier name, String type, Optional<String> comment)
    {
        this(Optional.of(location), name, type, comment);
    }

    private ColumnDefinition(Optional<NodeLocation> location, Identifier name, String type, Optional<String> comment)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitColumnDefinition(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        ColumnDefinition o = (ColumnDefinition) obj;
        return Objects.equals(this.name, o.name) &&
                Objects.equals(this.type, o.type) &&
                Objects.equals(this.comment, o.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("comment", comment)
                .toString();
    }
}
