package org.mortbay.sailing.pf.importer;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/**
 * Captures importer messages to a per-importer log file
 * ({@code <dataRoot>/log/<importerName>.log}) in addition to the normal SLF4J output,
 * and optionally to an in-memory {@link Capture} for a single run.
 * <p>
 * Usage: call {@link #open} before running an importer, {@link #close} after (in a
 * finally block), and replace {@code LOG.warn}/{@code LOG.error} in importer classes
 * with {@code ImporterLog.warn}/{@code ImporterLog.error}. Progress messages worth
 * keeping in the run log go through {@link #info} (which also emits at SLF4J INFO).
 * <p>
 * A {@link Capture} opened with {@link #openCapture} collects the same lines in memory
 * so a single on-demand run (e.g. the per-club import triggered from the clubs page)
 * can offer its full log as a download.
 */
public class ImporterLog
{
    private static final ThreadLocal<PrintWriter> FILE_LOG = new ThreadLocal<>();
    private static final ThreadLocal<Capture> CAPTURE = new ThreadLocal<>();
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * In-memory accumulation of one run's log lines, with WARN/ERROR tallies so a caller
     * can summarise the run without re-parsing the text.
     */
    public static class Capture
    {
        private final StringBuilder text = new StringBuilder();
        private int warnings;
        private int errors;

        synchronized void append(String level, String line)
        {
            text.append(line).append('\n');
            if ("WARN ".equals(level))
                warnings++;
            else if ("ERROR".equals(level))
                errors++;
        }

        public synchronized String text()
        {
            return text.toString();
        }

        public synchronized int warnings()
        {
            return warnings;
        }

        public synchronized int errors()
        {
            return errors;
        }
    }

    public static void open(Path logDir, String importerName)
    {
        try
        {
            Files.createDirectories(logDir);
            PrintWriter pw = new PrintWriter(Files.newBufferedWriter(
                logDir.resolve(importerName + ".log"),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND));
            pw.println("=== " + LocalDateTime.now() + " ===");
            pw.flush();
            FILE_LOG.set(pw);
        }
        catch (IOException e)
        {
            // Non-fatal: importer still runs, warnings go to SLF4J only
        }
    }

    public static void close()
    {
        PrintWriter pw = FILE_LOG.get();
        if (pw != null)
        {
            pw.close();
            FILE_LOG.remove();
        }
    }

    /**
     * Starts capturing this thread's importer log lines in memory. The returned Capture
     * keeps accumulating until {@link #closeCapture} is called; its contents remain
     * readable afterwards.
     */
    public static Capture openCapture()
    {
        Capture capture = new Capture();
        CAPTURE.set(capture);
        return capture;
    }

    public static void closeCapture()
    {
        CAPTURE.remove();
    }

    public static void info(Logger log, String msg, Object... args)
    {
        log.info(msg, args);
        write("INFO ", msg, args);
    }

    public static void warn(Logger log, String msg, Object... args)
    {
        log.warn(msg, args);
        write("WARN ", msg, args);
    }

    public static void error(Logger log, String msg, Object... args)
    {
        log.error(msg, args);
        write("ERROR", msg, args);
    }

    private static void write(String level, String msg, Object[] args)
    {
        PrintWriter pw = FILE_LOG.get();
        Capture capture = CAPTURE.get();
        if (pw == null && capture == null)
            return;
        String formatted = MessageFormatter.arrayFormat(msg, args).getMessage();
        String line = LocalDateTime.now().format(TIME_FMT) + " " + level + " " + formatted;
        // INFO is progress noise for the shared per-importer log file; it is only kept
        // in the in-memory capture that backs a single on-demand run's downloadable log.
        if (pw != null && !"INFO ".equals(level))
        {
            pw.println(line);
            pw.flush();
        }
        if (capture != null)
            capture.append(level, line);
    }
}
