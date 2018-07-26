package ideal.sylph.runner.spark;

import ideal.sylph.runner.spark.etl.structured.StructuredPluginLoader;
import ideal.sylph.spi.App;
import ideal.sylph.spi.NodeLoader;
import ideal.sylph.spi.job.Flow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * test demo
 */
@Deprecated
public final class SparkAppUtil
{
    private SparkAppUtil() {}

    public static void main(String[] args)
            throws Exception
    {
        System.out.println("spark on yarn app starting...");
        final SparkSession spark = SparkSession.builder().getOrCreate();

        try (ServerSocket sock = new ServerSocket()) {
            sock.bind(new InetSocketAddress(InetAddress.getLocalHost(), 7102));
            try (Socket client = sock.accept()) {
                try (InputStream input = client.getInputStream(); ObjectInputStream os = new ObjectInputStream(input)) {
                    Flow flow = (Flow) os.readObject();
                    App<SparkSession, Dataset<Row>> app = new App<SparkSession, Dataset<Row>>()
                    {
                        @Override
                        public NodeLoader<SparkSession, Dataset<Row>> getNodeLoader()
                        {
                            return new StructuredPluginLoader();
                        }

                        @Override
                        public SparkSession getContext()
                        {
                            return spark;
                        }
                    };
                    app.build("spark on yarn compile", flow).run();
                }
            }
        }

        checkArgument(spark.streams().active().length > 0, "无结构化流");
        spark.streams().awaitAnyTermination();
    }
}
