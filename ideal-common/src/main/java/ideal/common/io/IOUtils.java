/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.common.io;

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
