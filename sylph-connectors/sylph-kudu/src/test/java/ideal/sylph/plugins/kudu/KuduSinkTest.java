package ideal.sylph.plugins.kudu;

import com.github.harbby.gadtry.ioc.IocFactory;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SinkContext;
import ideal.sylph.parser.antlr.AntlrSqlParser;
import ideal.sylph.parser.antlr.tree.CreateTable;
import ideal.sylph.runner.flink.actuator.StreamSqlBuilder;
import ideal.sylph.runner.flink.etl.FlinkNodeLoader;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static ideal.sylph.runner.flink.actuator.StreamSqlUtil.getTableSchema;

public class KuduSinkTest
{
    private static final AntlrSqlParser sqlParser = new AntlrSqlParser();

    private final String kuduSinkSql = "create output table kudu(\n" +
            "    key varchar,\n" +
            "    value varchar\n" +
            ") with (\n" +
            "    type = '" + KuduSink.class.getName() + "',\n" +
            "    kudu.hosts = 'localhost:7051',\n" +
            "    kudu.tableName = 'impala::a1.a1',\n" +
            "    batchSize = 100\n" +
            ")";

    public static StreamTableEnvironment getTableEnv()
    {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
        execEnv.setParallelism(2);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnv);
        return tableEnv;
    }

    @Test
    public void createKuduSinkTest()
            throws ClassNotFoundException
    {
        CreateTable createStream = (CreateTable) sqlParser.createStatement(kuduSinkSql);
        final String tableName = createStream.getName();
        Schema schema = getTableSchema(createStream);

        final Map<String, Object> withConfig = createStream.getWithConfig();
        final String driverClass = (String) withConfig.get("type");

        final IocFactory iocFactory = IocFactory.create(binder -> binder.bind(SinkContext.class, new SinkContext()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public String getSinkTable()
            {
                return tableName;
            }
        }));
        NodeLoader<DataStream<Row>> loader = new FlinkNodeLoader(PipelinePluginManager.getDefault(), iocFactory);

        KuduSink kuduSink = (KuduSink) loader.getPluginInstance(Class.forName(driverClass), withConfig);
        Assert.assertTrue(kuduSink != null);
    }

    @Test
    public void createKuduSink()
            throws Exception
    {
        StreamTableEnvironment tableEnv = getTableEnv();

        StreamSqlBuilder streamSqlBuilder = new StreamSqlBuilder(tableEnv, PipelinePluginManager.getDefault(), sqlParser);
        streamSqlBuilder.buildStreamBySql(kuduSinkSql);

        tableEnv.sqlUpdate("insert into kudu select 'key' as key, '' as `value`");
        Assert.assertNotNull(tableEnv.execEnv().getStreamGraph().getJobGraph());
    }
}