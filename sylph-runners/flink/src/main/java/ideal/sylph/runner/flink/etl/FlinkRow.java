package ideal.sylph.runner.flink.etl;

import ideal.sylph.api.Row;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.List;
import java.util.Map;

public class FlinkRow
        implements Row
{
    private org.apache.flink.types.Row row;
    private final TypeInformation<org.apache.flink.types.Row> typeInformation;

    public FlinkRow(org.apache.flink.types.Row row, TypeInformation<org.apache.flink.types.Row> typeInformation)
    {
        this.row = row;
        this.typeInformation = typeInformation;
    }

    public org.apache.flink.types.Row get()
    {
        return this.row;
    }

    @Override
    public String mkString(String seq)
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < row.getArity(); i++) {
            stringBuilder.append(seq).append(row.getField(i));
        }
        return stringBuilder.substring(1);
    }

    @Override
    public String mkString()
    {
        return this.mkString(",");
    }

    @Override
    public <T> T getAs(String key)
    {
        if (typeInformation instanceof RowTypeInfo) {
            int index = ((RowTypeInfo) typeInformation).getFieldIndex(key);
            return (T) row.getField(index);
        }
        else {
            throw new IllegalStateException("typeInformation not is RowTypeInfo");
        }
    }

    @Override
    public <T> T getAs(int i)
    {
        return (T) row.getField(i);
    }

    @Override
    public int size()
    {
        return row.getArity();
    }

    @Override
    public String toString()
    {
        return row.toString();
    }

    public static org.apache.flink.types.Row parserRow(Row row)
    {
        if (row instanceof FlinkRow) {
            return ((FlinkRow) row).get();
        }
        else if (row instanceof GenericRowWithSchema) {
            return org.apache.flink.types.Row.of(((GenericRowWithSchema) row).getValues());
        }
        else {
            throw new RuntimeException(" not souch row type: " + row.getClass());
        }
    }

    public static RowTypeInfo parserRowType(Schema schema)
    {
        String[] fieldNames = schema.getFields().stream().map(Field::getName).toArray(String[]::new);
        return new RowTypeInfo(schema.getFields().stream().map(field -> {
            Class<?> javaType = field.getJavaType();
            return parserType(javaType);
        }).toArray(TypeInformation[]::new), fieldNames);
    }

    private static TypeInformation<?> parserType(Class<?> javaType)
    {
        if (javaType == String.class) {
            return TypeExtractor.createTypeInfo(javaType);
        }
        else if (javaType == byte.class) {
            return TypeExtractor.createTypeInfo(javaType);
        }
        else if (javaType == short.class) {
            return TypeExtractor.createTypeInfo(javaType);
        }
        else if (javaType == int.class) {
            return TypeExtractor.createTypeInfo(javaType);
        }
        else if (javaType == long.class) {
            return TypeExtractor.createTypeInfo(javaType);
        }
        else if (javaType == double.class) {
            return TypeExtractor.createTypeInfo(javaType);
        }
        else if (javaType == float.class) {
            return TypeExtractor.createTypeInfo(javaType);
        }
        else if (javaType == Map.class) {
            return TypeExtractor.createTypeInfo(javaType);
        }
        else if (javaType == List.class) {
            return TypeExtractor.createTypeInfo(javaType);
        }
        else {
            return TypeExtractor.createTypeInfo(javaType);
        }
    }
}
