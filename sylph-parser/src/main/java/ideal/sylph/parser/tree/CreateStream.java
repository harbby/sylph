package ideal.sylph.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateStream
        extends Statement
{
    public enum Type
    {
        SINK,
        SOURCE
    }

    private final QualifiedName name;
    private final List<TableElement> elements;
    private final boolean notExists;
    private final List<Property> properties;
    private final Optional<String> comment;
    private final Type type;

    public CreateStream(Type type, NodeLocation location, QualifiedName name, List<TableElement> elements, boolean notExists, List<Property> properties, Optional<String> comment)
    {
        this(type, Optional.of(location), name, elements, notExists, properties, comment);
    }

    private CreateStream(Type type, Optional<NodeLocation> location, QualifiedName name, List<TableElement> elements, boolean notExists, List<Property> properties, Optional<String> comment)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
        this.notExists = notExists;
        this.properties = requireNonNull(properties, "properties is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.type = type;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<TableElement> getElements()
    {
        return elements;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateStream(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(elements)
                .addAll(properties)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, elements, notExists, properties, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CreateStream o = (CreateStream) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(elements, o.elements) &&
                Objects.equals(notExists, o.notExists) &&
                Objects.equals(properties, o.properties) &&
                Objects.equals(comment, o.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("elements", elements)
                .add("notExists", notExists)
                .add("properties", properties)
                .add("type", type)
                .add("comment", comment)
                .toString();
    }
}
