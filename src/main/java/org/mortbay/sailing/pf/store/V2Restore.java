package org.mortbay.sailing.pf.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.mortbay.sailing.pf.data.Race;

/**
 * After the purged races have been re-imported, puts back those that did not come back: each
 * race under {@code purged/races/} (kept there by {@link V2Migration}) whose ID the database
 * does not hold is restored as it was converted. So the database ends with the re-imported
 * races plus exactly those that could not be got back. Run with the server stopped.
 * <p>
 * Usage: {@code V2Restore [--dry-run] <v2-root>}.
 */
public final class V2Restore
{
    private static final JsonMapper JSON = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();

    /** Purged races put back (not re-imported) and found re-imported, by source. */
    public record Result(Map<String, Integer> restored, Map<String, Integer> reimported)
    {
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("v2 restore of purged races\n  source: restored / re-imported\n");
            Map<String, int[]> all = new TreeMap<>();
            restored.forEach((k, v) -> all.computeIfAbsent(k, x -> new int[2])[0] = v);
            reimported.forEach((k, v) -> all.computeIfAbsent(k, x -> new int[2])[1] = v);
            all.forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v[0]).append(" / ").append(v[1]).append('\n'));
            return sb.toString();
        }
    }

    public static void main(String[] args) throws IOException
    {
        boolean dryRun = args.length == 2 && args[0].equals("--dry-run");
        if (args.length != (dryRun ? 2 : 1))
        {
            System.err.println("Usage: V2Restore [--dry-run] <v2-root>");
            System.exit(2);
        }
        DataStore store = new DataStore(Path.of(args[args.length - 1]));
        store.start();
        try
        {
            System.out.print(restore(store, dryRun));
        }
        finally
        {
            store.stop();
        }
    }

    /** Restores into a started store the purged races it does not hold, and saves it. */
    public static Result restore(DataStore store, boolean dryRun) throws IOException
    {
        Path purgedDir = store.dataRoot().resolve("purged/races");
        Map<String, Integer> restored = new TreeMap<>();
        Map<String, Integer> reimported = new TreeMap<>();
        if (!Files.isDirectory(purgedDir))
            return new Result(restored, reimported);
        try (Stream<Path> files = Files.walk(purgedDir))
        {
            for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList())
            {
                Race race = JSON.readValue(f.toFile(), Race.class);
                String source = sourceName(race.source());
                if (store.races().containsKey(race.id()))
                {
                    reimported.merge(source, 1, Integer::sum);
                    continue;
                }
                restored.merge(source, 1, Integer::sum);
                if (!dryRun)
                    store.putRace(race);
            }
        }
        if (!dryRun)
            store.save();
        return new Result(restored, reimported);
    }

    /** The importer's name in a race source: "SailSys-7473" or "TopYacht - url" gives SailSys or TopYacht. */
    static String sourceName(String source)
    {
        if (source == null || source.isBlank())
            return "(none)";
        String s = source.trim();
        if (s.startsWith("MYC spreadsheet"))
            return "MYC spreadsheet";
        return s.split("[^A-Za-z]")[0];
    }

    private V2Restore()
    {
    }
}
