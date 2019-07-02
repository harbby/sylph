package ideal.sylph.cli;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.Completer;
import jline.console.history.History;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public class LineReader
        extends ConsoleReader
        implements Closeable
{
    private boolean interrupted;

    LineReader(History history, Completer... completers)
            throws IOException
    {
        setExpandEvents(false);
        setBellEnabled(true);
        setHandleUserInterrupt(true);
        setHistory(history);
        setHistoryEnabled(false);
        for (Completer completer : completers) {
            addCompleter(completer);
        }
    }

    @Override
    public String readLine(String prompt, Character mask)
            throws IOException
    {
        String line;
        interrupted = false;
        try {
            line = super.readLine(prompt, mask);
        }
        catch (UserInterruptException e) {
            interrupted = true;
            return null;
        }

        if (getHistory() instanceof Flushable) {
            ((Flushable) getHistory()).flush();
        }
        return line;
    }

    @Override
    public void close()
    {
        super.close();
    }

    public boolean interrupted()
    {
        return interrupted;
    }
}
