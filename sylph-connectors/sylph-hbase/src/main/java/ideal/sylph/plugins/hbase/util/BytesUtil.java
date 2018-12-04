package ideal.sylph.plugins.hbase.util;

import ideal.sylph.plugins.hbase.HbaseSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class BytesUtil {
    private static Logger logger = LoggerFactory.getLogger(HbaseSink.class);

    /**
     * Convert object to byte[].
     * @param obj Object that need to be converted.
     * @return byte[].
    * */
    public static byte[] toBytes(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof String) {
            return ((String)obj).getBytes();
        } else if (obj instanceof Integer) {
            return Bytes.toBytes((Integer)obj);
        } else if (obj instanceof Long) {
            return Bytes.toBytes((Long)obj);
        } else if (obj instanceof Short) {
            return Bytes.toBytes((Short)obj);
        } else if (obj instanceof Float) {
            return Bytes.toBytes((Float)obj);
        } else if (obj instanceof Double) {
            return Bytes.toBytes((Double)obj);
        } else if (obj instanceof Boolean) {
            return Bytes.toBytes((Boolean)obj);
        } else if (obj instanceof BigDecimal) {
            return Bytes.toBytes((BigDecimal)obj);
        } else {
            logger.error("Can't convert class to byte array: " + obj.getClass().getName());
            return new byte[0];
        }
    }
}
