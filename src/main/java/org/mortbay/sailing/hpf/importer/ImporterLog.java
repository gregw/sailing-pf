package org.mortbay.sailing.hpf.importer;

import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.PrintWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Captures importer WARN and ERROR messages to a per-importer log file
 * ({@code <dataRoot>/log/<importerName>.log}) in addition to the normal SLF4J output.
 * <p>
 * Usage: call {@link #open} before running an importer, {@link #close} after (in a
 * finally block), and replace {@code LOG.warn}/{@code LOG.error} in importer classes
 * with {@code ImporterLog.warn}/{@code ImporterLog.error}.
 */
public class ImporterLog
{
    private static final ThreadLocal<PrintWriter> FILE_LOG = new ThreadLocal<>();
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

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
        if (pw == null)
            return;
        String formatted = MessageFormatter.arrayFormat(msg, args).getMessage();
        pw.println(LocalDateTime.now().format(TIME_FMT) + " " + level + " " + formatted);
        pw.flush();
    }
}
