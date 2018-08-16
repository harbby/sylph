package ideal.sylph.runner.flink.actuator;

import com.google.common.collect.ImmutableMap;
import ideal.sylph.parser.SqlParser;
import ideal.sylph.parser.tree.ColumnDefinition;
import ideal.sylph.parser.tree.CreateStream;
import ideal.sylph.runner.flink.etl.FlinkNodeLoader;
import ideal.sylph.runner.flink.table.SylphTableSink;
import ideal.sylph.runner.flink.table.SylphTableSource;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static ideal.sylph.parser.tree.CreateStream.Type.SINK;
import static ideal.sylph.parser.tree.CreateStream.Type.SOURCE;

public class StreamSqlUtil
{
    private StreamSqlUtil() {}

    public static void runCreateTableSql(PipelinePluginManager pluginManager, StreamTableEnvironment tableEnv, SqlParser sqlParser, String sql)
    {
        CreateStream createStream = (CreateStream) sqlParser.createStatement(sql);
        String tableName = createStream.getName().toString();

        List<ColumnDefinition> columns = createStream.getElements().stream().map(ColumnDefinition.class::cast).collect(Collectors.toList());

        Map<String, String> withConfig = createStream.getProperties().stream()
                .collect(Collectors.toMap(
                        k -> k.getName().getValue(),
                        v -> v.getValue().toString().replace("'", ""))
                );

        Map<String, Object> config = ImmutableMap.<String, Object>builder()
                .putAll(withConfig)
                .put("driver", withConfig.get("type"))
                .put("tableName", tableName)
                .build();
        NodeLoader<StreamTableEnvironment, DataStream<Row>> loader = new FlinkNodeLoader(pluginManager);
        RowTypeInfo rowTypeInfo = parserColumns(columns);
        if (SOURCE == createStream.getType()) {  //Source.class.isAssignableFrom(driver)
            UnaryOperator<DataStream<Row>> inputStream = loader.loadSource(tableEnv, config);
            SylphTableSource tableSource = new SylphTableSource(rowTypeInfo, inputStream);
            tableEnv.registerTableSource(tableName, tableSource);
//            System.out.println(rowTypeInfo);
        }
        else if (SINK == createStream.getType()) {
            UnaryOperator<DataStream<Row>> outputStream = loader.loadSink(config);
            SylphTableSink tableSink = new SylphTableSink(rowTypeInfo, outputStream);
            tableEnv.registerTableSink(tableName, tableSink.getFieldNames(), tableSink.getFieldTypes(), tableSink);
        }
        else {
            throw new IllegalArgumentException("this driver class " + withConfig.get("type") + " have't support!");
        }
    }

    private static RowTypeInfo parserColumns(List<ColumnDefinition> columns)
    {
        String[] fieldNames = columns.stream().map(columnDefinition -> columnDefinition.getName().getValue())
                .toArray(String[]::new);

        TypeInformation[] fieldTypes = columns.stream().map(columnDefinition -> parserSqlType(columnDefinition.getType()))
                .toArray(TypeInformation[]::new);

        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    private static TypeInformation<?> parserSqlType(String type)
    {
        switch (type) {
            case "varchar":
            case "string":
                return Types.STRING();
            case "integer":
            case "int":
                return Types.INT();
            case "long":
            case "bigint":
                return Types.LONG();
            case "boolean":
            case "bool":
                return Types.BOOLEAN();
            case "double":
                return Types.DOUBLE();
            case "float":
                return Types.FLOAT();
            case "byte":
                return Types.BYTE();
            case "timestamp":
                return Types.SQL_TIMESTAMP();
            case "date":
                return Types.SQL_DATE();
            case "binary":
                return TypeExtractor.createTypeInfo(byte[].class); //Types.OBJECT_ARRAY(Types.BYTE());
            default:
                throw new IllegalArgumentException("this TYPE " + type + " have't support!");
        }
    }
}
