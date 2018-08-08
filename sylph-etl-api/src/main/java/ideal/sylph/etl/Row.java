package ideal.sylph.etl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public interface Row
{
    String mkString(String seq);

    default String mkString()
    {
        return this.mkString(",");
    }

    <T> T getAs(String key);

    <T> T getAs(int i);

    default <T> T getField(int i)
    {
        return getAs(i);
    }

    int size();

    public static Row of(Object[] values)
    {
        return new DefaultRow(values);
    }

    static class DefaultRow
            implements Row
    {
        Object[] values;

        private DefaultRow(Object[] values)
        {
            this.values = values;
        }

        public Object[] getValues()
        {
            return Arrays.copyOf(values, values.length);
        }

        @Override
        public String mkString(String seq)
        {
            throw new UnsupportedOperationException("this " + this.getClass().getName() + " method have't mkString!");
        }

        @Override
        public String mkString()
        {
            throw new UnsupportedOperationException("this " + this.getClass().getName() + " method have't mkString!");
        }

        @Override
        public <T> T getAs(String key)
        {
            throw new UnsupportedOperationException("this " + this.getClass().getName() + " method have't T getAs(String)!");
        }

        @Override
        public <T> T getAs(int key)
        {
            throw new UnsupportedOperationException("this " + this.getClass().getName() + " method have't T getAs(int)!");
        }

        @Override
        public int size()
        {
            return values.length;
        }
    }

    public static final class Schema
    {
        private final List<Field> fields;

        private Schema(List<Field> fields)
        {
            this.fields = requireNonNull(fields, "fields must not null");
        }

        public List<Field> getFields()
        {
            return fields;
        }

        public static SchemaBuilder newBuilder()
        {
            return new SchemaBuilder();
        }

        public static class SchemaBuilder
        {
            private final List<Field> fields = new ArrayList<>();

            public SchemaBuilder add(String name, Class<?> javaType)
            {
                fields.add(new Field(name, javaType));
                return this;
            }

            public Schema build()
            {
                return new Schema(fields.stream().collect(Collectors.toList()));
            }
        }
    }

    public static final class Field
    {
        private final String name;
        private final Class<?> javaType;

        private Field(String name, Class<?> javaType)
        {
            this.name = requireNonNull(name, "Field name must not null");
            this.javaType = requireNonNull(javaType, "Field type must not null");
        }

        public String getName()
        {
            return name;
        }

        public Class<?> getJavaType()
        {
            return javaType;
        }
    }
}
