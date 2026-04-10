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
import java.util.Optional;

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
        public String canonicalSailNumber;              // canonical sail number for this boat
        public List<AliasEntry> aliases;                // name aliases only
        public List<AltSailNumberEntry> altSailNumbers; // alternate sail numbers
    }

    static class AliasEntry
    {
        public String name;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        public LocalDate from;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        public LocalDate until;
    }

    static class AltSailNumberEntry
    {
        public String number;
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
        /** normalised sail number → timed aliases (keyed by canonicalSailNumber) */
        private final Map<String, List<TimedAlias>> boatAliasMap;
        /** normalised sail number → canonical boat name (keyed by canonicalSailNumber) */
        private final Map<String, String> boatCanonicalNames;
        /** normalised typo/old sail number → normalised canonical sail number */
        private final Map<String, String> sailNumberRedirects;
        /**
         * normalised sail number (canonical + all alternates) → entry.
         * Used to look up entries by any known sail number.
         */
        private final Map<String, BoatSeedEntry> sailNumberIndex;
        /**
         * normName[-designId] → entry.
         * Used to look up entries by canonical name + design.
         */
        private final Map<String, BoatSeedEntry> nameDesignIndex;

        private AliasSeed(SeedFile seed)
        {
            if (seed == null || (seed.designs == null && seed.boats == null && seed.sailNumberRedirects == null))
            {
                designAliasIndex = Map.of();
                designCanonicalNames = Map.of();
                boatAliasMap = Map.of();
                boatCanonicalNames = Map.of();
                sailNumberRedirects = Map.of();
                sailNumberIndex = Map.of();
                nameDesignIndex = Map.of();
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
            Map<String, BoatSeedEntry> sailIdx = new HashMap<>();
            Map<String, BoatSeedEntry> nameDesignIdx = new HashMap<>();

            if (seed.boats != null)
            {
                for (Map.Entry<String, BoatSeedEntry> e : seed.boats.entrySet())
                {
                    String yamlKey = e.getKey();
                    BoatSeedEntry entry = e.getValue();

                    // Determine the canonical sail number for boatAliasMap/boatCanonicalNames lookups.
                    // New schema: entry.canonicalSailNumber is explicit.
                    // Legacy schema (key = sail number): use the key as the canonical sail number.
                    String canonicalSail = entry.canonicalSailNumber != null
                        ? IdGenerator.normaliseSailNumber(entry.canonicalSailNumber)
                        : IdGenerator.normaliseSailNumber(yamlKey);

                    if (entry.canonicalName != null)
                        boatNames.put(canonicalSail, entry.canonicalName);

                    if (entry.aliases != null)
                    {
                        List<TimedAlias> timedAliases = entry.aliases.stream()
                            .filter(a -> a.name != null)
                            .map(a -> new TimedAlias(a.name, a.from, a.until))
                            .toList();
                        boatMap.put(canonicalSail, timedAliases);
                    }

                    // Index by canonical sail number
                    sailIdx.put(canonicalSail, entry);

                    // Index by alternate sail numbers
                    if (entry.altSailNumbers != null)
                    {
                        for (AltSailNumberEntry alt : entry.altSailNumbers)
                        {
                            if (alt.number != null)
                                sailIdx.put(IdGenerator.normaliseSailNumber(alt.number), entry);
                        }
                    }

                    // Index by normName[-designId] derived from YAML key
                    // New keys are already in normName or normName-designId form
                    // Legacy keys (sail numbers) will produce a non-useful nameDesign key but that's harmless
                    String normKey = yamlKey.toLowerCase().replace(' ', '_').replace('-', '_');
                    // For new-style keys (not a sail number), use them as-is for nameDesign lookup
                    // We detect new-style keys by checking if canonicalSailNumber is explicitly set
                    if (entry.canonicalSailNumber != null)
                    {
                        // New-style key: use yamlKey as the nameDesign key
                        nameDesignIdx.put(yamlKey, entry);
                    }
                    else if (entry.canonicalName != null)
                    {
                        // Legacy key (sail number): also index by normName so lookupBoat can find it
                        String normName = IdGenerator.normaliseName(entry.canonicalName);
                        nameDesignIdx.put(normName, entry);
                    }
                }
            }
            boatAliasMap = Collections.unmodifiableMap(boatMap);
            boatCanonicalNames = Collections.unmodifiableMap(boatNames);
            sailNumberIndex = Collections.unmodifiableMap(sailIdx);
            nameDesignIndex = Collections.unmodifiableMap(nameDesignIdx);
            LOG.debug("Boat alias sailNumberIndex keys: {}", sailIdx.keySet());
            LOG.debug("Boat alias nameDesignIndex keys: {}", nameDesignIdx.keySet());

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

        /**
         * Result of a boat seed lookup.
         */
        public record BoatSeedMatch(String canonicalName, String canonicalSailNumber) {}

        /**
         * Looks up a boat entry by sail number or by name+design key.
         * Returns a BoatSeedMatch if found, empty if not.
         */
        Optional<BoatSeedMatch> lookupBoat(String normSailNumber, String nameDesignKey)
        {
            BoatSeedEntry e = sailNumberIndex.get(normSailNumber);
            if (e == null)
                e = nameDesignIndex.get(nameDesignKey);
            if (e == null)
                return Optional.empty();
            String canonicalSail = e.canonicalSailNumber != null
                ? IdGenerator.normaliseSailNumber(e.canonicalSailNumber)
                : normSailNumber;
            return Optional.of(new BoatSeedMatch(e.canonicalName, canonicalSail));
        }
    }

    // ---- Alias file update (called after a merge operation) ----

    /**
     * Reads aliases.yaml, merges in design alias entries from a merge operation, and writes back.
     * For each alias name (canonical names and IDs of the merged-away designs), adds an entry
     * under the keep design's ID in the {@code designs:} section.
     */
    public static void appendDesignMergeAliases(Path configDir, String keepId, String canonicalName, List<String> aliasNames)
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
        if (seedFile.designs == null)
            seedFile.designs = new LinkedHashMap<>();

        DesignSeedEntry entry = seedFile.designs.computeIfAbsent(keepId, k -> new DesignSeedEntry());
        if (entry.canonicalName == null && canonicalName != null)
            entry.canonicalName = canonicalName;
        if (entry.aliases == null)
            entry.aliases = new ArrayList<>();
        for (String name : aliasNames)
        {
            boolean present = entry.aliases.stream().anyMatch(a -> name.equalsIgnoreCase(a));
            if (!present)
                entry.aliases.add(name);
        }

        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), seedFile);
            LOG.info("Updated {} with design merge aliases for keepId={}", file, keepId);
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }

    /**
     * Spec for one merged-away boat: what alias entries to add to aliases.yaml.
     */
    public record MergeAliasSpec(
        String sailNumber,          // normalised sail number of the merged-away boat
        String canonicalSailNumber, // normalised sail number of the keep boat (may be equal)
        String canonicalName,       // canonical name of the keep boat
        String designId,            // design ID of the keep boat (nullable)
        List<String> aliasNames     // names to record as aliases under sailNumber
    ) {}

    /**
     * Reads aliases.yaml, merges in entries from a merge operation (skipping existing entries),
     * and writes the file back.  Pre-existing comments in the file are not preserved.
     * <p>
     * Uses the new name-design key scheme: entries are keyed by normName[-designId].
     * Alternate sail numbers (merged-away boat's sail number when different from canonical) are
     * stored in altSailNumbers rather than sailNumberRedirects.
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
            // Build the new-style YAML key: normName[-designId]
            String normName = IdGenerator.normaliseName(spec.canonicalName());
            String yamlKey = (spec.designId() != null && !spec.designId().isBlank())
                ? normName + "-" + spec.designId()
                : normName;

            // Find or create entry at the new key
            BoatSeedEntry entry = seedFile.boats.computeIfAbsent(yamlKey, k ->
            {
                BoatSeedEntry e = new BoatSeedEntry();
                e.canonicalName = spec.canonicalName();
                e.canonicalSailNumber = spec.canonicalSailNumber();
                e.aliases = new ArrayList<>();
                e.altSailNumbers = new ArrayList<>();
                return e;
            });
            if (entry.canonicalName == null)
                entry.canonicalName = spec.canonicalName();
            if (entry.canonicalSailNumber == null)
                entry.canonicalSailNumber = spec.canonicalSailNumber();
            if (entry.aliases == null)
                entry.aliases = new ArrayList<>();
            if (entry.altSailNumbers == null)
                entry.altSailNumbers = new ArrayList<>();

            // Add name aliases
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

            // Add alt sail number if the merged-away sail differs from the canonical
            if (!spec.sailNumber().equals(spec.canonicalSailNumber()))
            {
                boolean altPresent = entry.altSailNumbers.stream()
                    .anyMatch(a -> spec.sailNumber().equalsIgnoreCase(
                        a.number != null ? IdGenerator.normaliseSailNumber(a.number) : ""));
                if (!altPresent)
                {
                    AltSailNumberEntry alt = new AltSailNumberEntry();
                    alt.number = spec.sailNumber();
                    entry.altSailNumbers.add(alt);
                }

                // Remove the old sail-number-keyed entry if it exists (cleanup legacy entries)
                seedFile.boats.remove(spec.sailNumber());
                // Remove the old sailNumberRedirect for this sail number
                seedFile.sailNumberRedirects.remove(spec.sailNumber());
            }
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

    /**
     * Renames all boat alias YAML keys ending in {@code -oldDesignId} to {@code -newDesignId}.
     * Called after a design merge to keep YAML keys consistent with boat design IDs.
     */
    public static void updateBoatAliasKeysForDesignMerge(Path configDir, String oldDesignId, String newDesignId)
    {
        Path file = configDir.resolve(FILENAME);
        if (!Files.exists(file))
            return;

        SeedFile seedFile;
        try
        {
            seedFile = YAML_MAPPER.readValue(file.toFile(), SeedFile.class);
        }
        catch (Exception e)
        {
            LOG.error("Failed to read {} for design merge key update: {}", file, e.getMessage());
            return;
        }

        if (seedFile.boats == null || seedFile.boats.isEmpty())
            return;

        String suffix = "-" + oldDesignId;
        String newSuffix = "-" + newDesignId;
        Map<String, BoatSeedEntry> updated = new LinkedHashMap<>();
        boolean changed = false;
        for (Map.Entry<String, BoatSeedEntry> e : seedFile.boats.entrySet())
        {
            String key = e.getKey();
            if (key.endsWith(suffix))
            {
                String newKey = key.substring(0, key.length() - suffix.length()) + newSuffix;
                updated.put(newKey, e.getValue());
                changed = true;
                LOG.info("Renamed boat alias key {} → {} due to design merge", key, newKey);
            }
            else
            {
                updated.put(key, e.getValue());
            }
        }

        if (!changed)
            return;

        seedFile.boats = updated;
        try
        {
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), seedFile);
            LOG.info("Updated {} boat alias key(s) for design merge {} → {}", file, oldDesignId, newDesignId);
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }
}
