package ideal.sylph.runner.spark;

import ideal.sylph.spi.App;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.StreamingContext;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * spark main input
 */
public final class SparkAppMain
{
    private SparkAppMain() {}

    public static void main(String[] args)
            throws Exception
    {
        System.out.println("spark on yarn app starting...");

        SparkJobHandle<App<?, ?>> sparkJobHandle = null;
        try (ServerSocket sock = new ServerSocket()) {
            sock.bind(new InetSocketAddress(InetAddress.getLocalHost(), 7102));
            try (Socket client = sock.accept()) {
                try (InputStream input = client.getInputStream(); ObjectInputStream os = new ObjectInputStream(input)) {
                    sparkJobHandle = (SparkJobHandle<App<?, ?>>) os.readObject();
                }
            }
        }
        App<?, ?> app = requireNonNull(sparkJobHandle, "sparkJobHandle is null").getApp().get();
        app.build().run();
        Object appContext = app.getContext();
        if (appContext instanceof SparkSession) {
            checkArgument(((SparkSession) appContext).streams().active().length > 0, "no stream pipeline");
            ((SparkSession) appContext).streams().awaitAnyTermination();
        }
        else if (appContext instanceof StreamingContext) {
            ((StreamingContext) appContext).start();
            ((StreamingContext) appContext).awaitTermination();
        }
    }
}
