package ideal.sylph.common.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

public class IOUtils
{
    private IOUtils() {}

    /**
     * Copies from one stream to another.
     *
     * @param in InputStrem to read from
     * @param out OutputStream to write to
     * @param buffSize the size of the buffer
     * @param close whether or not close the InputStream and
     * OutputStream at the end. The streams are closed in the finally clause.
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close)
            throws IOException
    {
        if (close) {
            try (InputStream input = in; OutputStream output = out) {
                copyBytes(in, out, buffSize);
            }
        }
        else {
            copyBytes(in, out, buffSize);
        }
    }

    /**
     * Copies from one stream to another.
     *
     * @param in InputStrem to read from
     * @param out OutputStream to write to
     * @param buffSize the size of the buffer
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize)
            throws IOException
    {
        PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        byte[] buf = new byte[buffSize];
        int bytesRead = -1;
        while ((bytesRead = in.read(buf)) >= 0) {
            out.write(buf, 0, bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
        }
    }
}
