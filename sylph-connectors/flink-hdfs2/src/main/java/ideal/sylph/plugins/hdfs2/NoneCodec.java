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
package ideal.sylph.plugins.hdfs2;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class NoneCodec
        implements CompressionCodec
{
    @Override
    public CompressionOutputStream createOutputStream(OutputStream out)
            throws IOException
    {
        return new CompressionOutputStream(out)
        {
            @Override
            public void write(byte[] b, int off, int len)
                    throws IOException
            {
                out.write(b, off, len);
            }

            @Override
            public void write(int b)
                    throws IOException
            {
                out.write(b);
            }

            @Override
            public void finish()
                    throws IOException
            {
            }

            @Override
            public void resetState()
                    throws IOException
            {
            }
        };
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
            throws IOException
    {
        return this.createOutputStream(out);
    }

    @Override
    public Class<? extends Compressor> getCompressorType()
    {
        return null;
    }

    @Override
    public Compressor createCompressor()
    {
        return null;
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in)
            throws IOException
    {
        return null;
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
            throws IOException
    {
        return null;
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType()
    {
        return null;
    }

    @Override
    public Decompressor createDecompressor()
    {
        return null;
    }

    @Override
    public String getDefaultExtension()
    {
        return null;
    }
}
