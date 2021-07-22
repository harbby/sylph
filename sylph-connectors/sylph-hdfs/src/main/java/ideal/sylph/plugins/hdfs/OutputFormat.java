package ideal.sylph.plugins.hdfs;

import ideal.sylph.etl.Record;

import java.io.Closeable;
import java.io.IOException;

public interface OutputFormat
        extends Closeable
{
    public void writeLine(Record record)
            throws IOException;
}
