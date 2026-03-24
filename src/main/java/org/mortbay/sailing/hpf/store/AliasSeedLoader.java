package org.mortbay.sailing.hpf.store;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.mortbay.sailing.hpf.data.TimedAlias;
import org.mortbay.sailing.hpf.importer.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads {@code /aliases.yaml} from the classpath and provides lookup methods
 * for design and boat name aliases.
 * <p>
 * The loaded seed is lookup-only and is never written back to disk.
 */
public class AliasSeedLoader
{
    private static final Logger LOG = LoggerFactory.getLogger(AliasSeedLoader.class);
    private static final String FILENAME = "aliases.yaml";
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
        .registerModule(new JavaTimeModule());

    static AliasSeed load(Path configDir)
    {
        InputStream stream = openStream(configDir, FILENAME);
        if (stream == null)
        {
            LOG.warn("No aliases.yaml found; alias seed not loaded");
            return AliasSeed.EMPTY;
        }
        try
        {
            SeedFile seedFile = YAML_MAPPER.readValue(stream, SeedFile.class);
            return new AliasSeed(seedFile);
        }
        catch (Exception e)
        {
            LOG.error("Failed to load aliases.yaml: {}", e.getMessage(), e);
            return AliasSeed.EMPTY;
        }
    }

    private static InputStream openStream(Path configDir, String filename)
    {
        Path file = configDir.resolve(filename);
        if (Files.exists(file))
        {
            try
            {
                LOG.info("Loading {} from {}", filename, file.toAbsolutePath());
                return Files.newInputStream(file);
            }
            catch (Exception e)
            {
                LOG.warn("Failed to open {}: {}", file, e.getMessage());
            }
        }
        // Fallback to classpath (test resources)
        return AliasSeedLoader.class.getResourceAsStream("/" + filename);
    }

    // ---- YAML binding classes ----

    static class SeedFile
    {
        public Map<String, DesignSeedEntry> designs;
        public Map<String, BoatSeedEntry> boats;
        /** normSailNumber → normCanonicalSailNumber for boats with typo/old sail numbers. */
        public Map<String, String> sailNumberRedirects;
    }

    static class DesignSeedEntry
    {
        public String canonicalName;
        public List<String> aliases;
    }

    static class BoatSeedEntry
    {
        public String canonicalName;
        public List<AliasEntry> aliases;
    }

    static class AliasEntry
    {
        public String name;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        public LocalDate from;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        public LocalDate until;
    }

    // ---- Seed result ----

    /**
     * Loaded alias data. Immutable after construction; all lookups are O(1).
     */
    static class AliasSeed
    {
        static final AliasSeed EMPTY = new AliasSeed(null);

        /** normAlias → canonical design ID */
        private final Map<String, String> designAliasIndex;
        /** canonical design ID → canonical display name */
        private final Map<String, String> designCanonicalNames;
        /** normalised sail number → timed aliases */
        private final Map<String, List<TimedAlias>> boatAliasMap;
        /** normalised sail number → canonical boat name */
        private final Map<String, String> boatCanonicalNames;
        /** normalised typo/old sail number → normalised canonical sail number */
        private final Map<String, String> sailNumberRedirects;

        private AliasSeed(SeedFile seed)
        {
            if (seed == null || (seed.designs == null && seed.boats == null && seed.sailNumberRedirects == null))
            {
                designAliasIndex = Map.of();
                designCanonicalNames = Map.of();
                boatAliasMap = Map.of();
                boatCanonicalNames = Map.of();
                sailNumberRedirects = Map.of();
                return;
            }

            // Build design indexes
            Map<String, String> aliasIdx = new HashMap<>();
            Map<String, String> designNames = new HashMap<>();
            if (seed.designs != null)
            {
                for (Map.Entry<String, DesignSeedEntry> e : seed.designs.entrySet())
                {
                    String canonicalId = e.getKey();
                    DesignSeedEntry entry = e.getValue();
                    if (entry.canonicalName != null)
                        designNames.put(canonicalId, entry.canonicalName);
                    if (entry.aliases != null)
                    {
                        for (String alias : entry.aliases)
                        {
                            String normAlias = IdGenerator.normaliseDesignName(alias);
                            aliasIdx.put(normAlias, canonicalId);
                        }
                    }
                }
            }
            designAliasIndex = Collections.unmodifiableMap(aliasIdx);
            designCanonicalNames = Collections.unmodifiableMap(designNames);

            // Build boat indexes
            Map<String, List<TimedAlias>> boatMap = new HashMap<>();
            Map<String, String> boatNames = new HashMap<>();
            if (seed.boats != null)
            {
                for (Map.Entry<String, BoatSeedEntry> e : seed.boats.entrySet())
                {
                    String normSail = IdGenerator.normaliseSailNumber(e.getKey());
                    BoatSeedEntry entry = e.getValue();
                    if (entry.canonicalName != null)
                        boatNames.put(normSail, entry.canonicalName);
                    if (entry.aliases != null)
                    {
                        List<TimedAlias> timedAliases = entry.aliases.stream()
                            .filter(a -> a.name != null)
                            .map(a -> new TimedAlias(a.name, a.from, a.until))
                            .toList();
                        boatMap.put(normSail, timedAliases);
                    }
                }
            }
            boatAliasMap = Collections.unmodifiableMap(boatMap);
            boatCanonicalNames = Collections.unmodifiableMap(boatNames);

            // Build sail number redirect index
            Map<String, String> redirects = new HashMap<>();
            if (seed.sailNumberRedirects != null)
            {
                for (Map.Entry<String, String> e : seed.sailNumberRedirects.entrySet())
                {
                    String from = IdGenerator.normaliseSailNumber(e.getKey());
                    String to   = IdGenerator.normaliseSailNumber(e.getValue());
                    redirects.put(from, to);
                }
            }
            sailNumberRedirects = Collections.unmodifiableMap(redirects);

            LOG.info("Loaded alias seed: {} design alias(es), {} boat entry(ies), {} sail number redirect(s)",
                aliasIdx.size(), boatMap.size(), redirects.size());
        }

        /**
         * Resolves a normalised design alias to a canonical design ID, or null if unknown.
         */
        String resolveDesignAlias(String normalisedAlias)
        {
            return designAliasIndex.get(normalisedAlias);
        }

        /**
         * Returns the canonical display name for a design ID, or null if not in seed.
         */
        String designCanonicalName(String designId)
        {
            return designCanonicalNames.get(designId);
        }

        /**
         * Returns the timed aliases for a normalised sail number, or empty list if unknown.
         */
        List<TimedAlias> boatAliases(String normalisedSailNumber)
        {
            return boatAliasMap.getOrDefault(normalisedSailNumber, List.of());
        }

        /**
         * Returns the canonical boat name for a normalised sail number, or null if not in seed.
         */
        String boatCanonicalName(String normalisedSailNumber)
        {
            return boatCanonicalNames.get(normalisedSailNumber);
        }

        /**
         * Returns the canonical sail number for a typo/old sail number, or null if not in seed.
         * Used to redirect boats whose sail numbers were corrected post-merge.
         */
        String sailNumberRedirect(String normalisedSailNumber)
        {
            return sailNumberRedirects.get(normalisedSailNumber);
        }
    }

    // ---- Alias file update (called after a merge operation) ----

    /**
     * Spec for one merged-away boat: what alias entries to add to aliases.yaml.
     */
    public record MergeAliasSpec(
        String sailNumber,          // normalised sail number of the merged-away boat
        String canonicalSailNumber, // normalised sail number of the keep boat (may be equal)
        String canonicalName,       // canonical name of the keep boat
        List<String> aliasNames     // names to record as aliases under sailNumber
    ) {}

    /**
     * Reads aliases.yaml, merges in entries from a merge operation (skipping existing entries),
     * and writes the file back.  Pre-existing comments in the file are not preserved.
     */
    public static void appendMergeAliases(Path configDir, List<MergeAliasSpec> specs)
    {
        Path file = configDir.resolve(FILENAME);
        SeedFile seedFile = null;
        if (Files.exists(file))
        {
            try
            {
                seedFile = YAML_MAPPER.readValue(file.toFile(), SeedFile.class);
            }
            catch (Exception e)
            {
                LOG.error("Failed to read {} for update: {}", file, e.getMessage());
                return;
            }
        }
        if (seedFile == null)
            seedFile = new SeedFile();
        if (seedFile.boats == null)
            seedFile.boats = new LinkedHashMap<>();
        if (seedFile.sailNumberRedirects == null)
            seedFile.sailNumberRedirects = new LinkedHashMap<>();

        for (MergeAliasSpec spec : specs)
        {
            // Boat name alias entry — always add (even for different sail numbers, so the
            // canonical name is recorded for that sail number in case it reappears)
            BoatSeedEntry entry = seedFile.boats.computeIfAbsent(spec.sailNumber(), k ->
            {
                BoatSeedEntry e = new BoatSeedEntry();
                e.canonicalName = spec.canonicalName();
                e.aliases = new ArrayList<>();
                return e;
            });
            if (entry.canonicalName == null)
                entry.canonicalName = spec.canonicalName();
            if (entry.aliases == null)
                entry.aliases = new ArrayList<>();
            for (String name : spec.aliasNames())
            {
                boolean present = entry.aliases.stream().anyMatch(a -> name.equalsIgnoreCase(a.name));
                if (!present)
                {
                    AliasEntry ae = new AliasEntry();
                    ae.name = name;
                    entry.aliases.add(ae);
                }
            }

            // Sail number redirect — only when sail numbers differ
            if (!spec.sailNumber().equals(spec.canonicalSailNumber()))
                seedFile.sailNumberRedirects.putIfAbsent(spec.sailNumber(), spec.canonicalSailNumber());
        }

        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), seedFile);
            LOG.info("Updated {} with {} merge alias spec(s)", file, specs.size());
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }
}
