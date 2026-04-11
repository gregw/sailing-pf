package org.mortbay.sailing.hpf.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.mortbay.sailing.hpf.importer.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
public class AliasLoader
{
    private static final Logger LOG = LoggerFactory.getLogger(AliasLoader.class);
    private static final String FILENAME = "aliases.yaml";
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

    static Aliases load(Path configDir)
    {
        InputStream stream = openStream(configDir, FILENAME);
        if (stream == null)
        {
            LOG.warn("No aliases.yaml found; alias seed not loaded");
            return Aliases.EMPTY;
        }
        try
        {
            Yaml yaml = YAML_MAPPER.readValue(stream, Yaml.class);
            return new Aliases(yaml);
        }
        catch (Exception e)
        {
            LOG.error("Failed to load aliases.yaml: {}", e.getMessage(), e);
            return Aliases.EMPTY;
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
        return AliasLoader.class.getResourceAsStream("/" + filename);
    }

    // ---- YAML binding classes ----

    static class Yaml
    {
        public Map<String, DesignEntry> designs;
        public Map<String, BoatEntry> boats;
    }

    static class DesignEntry
    {
        public String canonicalName;
        public List<String> aliases;
    }

    static class BoatEntry
    {
        public String canonicalName;
        public String canonicalSailNumber;
        public List<Alias> aliases;
    }

    static class Alias
    {
        public String name;           // null means use canonicalName
        public String sailNumber;     // null means use canonicalSailNumber
    }

    // ---- Seed result ----

    /**
     * Loaded alias data. Immutable after construction; all lookups are O(1).
     */
    static class Aliases
    {
        static final Aliases EMPTY = new Aliases(null);

        /** normAlias → canonical design ID */
        private final Map<String, String> name2designId;
        /** canonical design ID → canonical display name */
        private final Map<String, String> designId2Name;
        /** normalised sail number → aliases (keyed by canonicalSailNumber) */
        private final Map<String, List<String>> sailNo2Aliases;
        /** normalised sail number → canonical boat name (keyed by canonicalSailNumber) */
        private final Map<String, String> boatName2CanonicalName;
        /**
         * normalised sail number (canonical + all alternates) → entry.
         * Used to look up entries by any known sail number.
         */
        private final Map<String, BoatEntry> sail2boat;
        /**
         * normName → entry.
         * YAML keys may have a {@code -designId} suffix for disambiguation in the file;
         * the suffix is stripped when building this index.
         */
        private final Map<String, BoatEntry> nameIndex;

        private Aliases(Yaml seed)
        {
            if (seed == null || (seed.designs == null && seed.boats == null))
            {
                name2designId = Map.of();
                designId2Name = Map.of();
                sailNo2Aliases = Map.of();
                boatName2CanonicalName = Map.of();
                sail2boat = Map.of();
                nameIndex = Map.of();
                return;
            }

            // Build design indexes
            Map<String, String> name2designIdMutable = new HashMap<>();
            Map<String, String> designId2NameMutable = new HashMap<>();
            if (seed.designs != null)
            {
                for (Map.Entry<String, DesignEntry> e : seed.designs.entrySet())
                {
                    String canonicalId = e.getKey();
                    DesignEntry entry = e.getValue();
                    if (entry.canonicalName != null)
                    {
                        designId2NameMutable.put(canonicalId, entry.canonicalName);
                        name2designIdMutable.put(IdGenerator.normaliseDesignName(entry.canonicalName), canonicalId);
                    }
                    if (entry.aliases != null)
                    {
                        for (String alias : entry.aliases)
                        {
                            String normAlias = IdGenerator.normaliseDesignName(alias);
                            name2designIdMutable.put(alias, canonicalId);
                            name2designIdMutable.put(normAlias, canonicalId);
                        }
                    }
                }
            }
            name2designId = Collections.unmodifiableMap(name2designIdMutable);
            designId2Name = Collections.unmodifiableMap(designId2NameMutable);

            // Build boat indexes
            Map<String, List<String>> boatMap = new HashMap<>();
            Map<String, String> boatNames = new HashMap<>();
            Map<String, BoatEntry> sail2boatMutable = new HashMap<>();
            Map<String, BoatEntry> nameIdx = new HashMap<>();

            if (seed.boats != null)
            {
                for (Map.Entry<String, BoatEntry> e : seed.boats.entrySet())
                {
                    String yamlKey = e.getKey();
                    BoatEntry entry = e.getValue();

                    // Key format: normSail-normName (e.g. "MYC7-daydreaming")
                    int dash = yamlKey.indexOf('-');
                    if (dash < 0)
                    {
                        LOG.warn("Skipping boat alias entry with invalid key (no dash): {}", yamlKey);
                        continue;
                    }
                    String canonicalSail = yamlKey.substring(0, dash);
                    String normName = yamlKey.substring(dash + 1);

                    if (entry.canonicalName != null)
                        boatNames.put(canonicalSail, entry.canonicalName);

                    if (entry.aliases != null)
                    {
                        List<String> nameAliases = entry.aliases.stream()
                            .filter(a -> a.name != null)
                            .map(a -> a.name)
                            .toList();
                        boatMap.put(canonicalSail, nameAliases);
                    }

                    // Index by canonical sail number
                    sail2boatMutable.put(canonicalSail, entry);

                    // Index by alternate sail numbers from aliases with a sailNumber field
                    if (entry.aliases != null)
                    {
                        for (Alias alias : entry.aliases)
                        {
                            if (alias.sailNumber != null)
                                sail2boatMutable.put(IdGenerator.normaliseSailNumber(alias.sailNumber), entry);
                        }
                    }

                    // Index by normName for fallback lookups
                    nameIdx.put(normName, entry);
                }
            }
            sailNo2Aliases = Collections.unmodifiableMap(boatMap);
            boatName2CanonicalName = Collections.unmodifiableMap(boatNames);
            sail2boat = Collections.unmodifiableMap(sail2boatMutable);
            nameIndex = Collections.unmodifiableMap(nameIdx);
            LOG.debug("Boat alias sailNumberIndex keys: {}", sail2boatMutable.keySet());
            LOG.debug("Boat alias nameIndex keys: {}", nameIdx.keySet());

            LOG.info("Loaded alias seed: {} design alias(es), {} boat entry(ies)",
                name2designIdMutable.size(), boatMap.size());
        }

        /**
         * Resolves a normalised design alias to a canonical design ID, or null if unknown.
         */
        String resolveDesignAlias(String normalisedAlias)
        {
            return name2designId.get(normalisedAlias);
        }

        /**
         * Returns the canonical display name for a design ID, or null if not in seed.
         */
        String designCanonicalName(String designId)
        {
            return designId2Name.get(designId);
        }

        /**
         * Returns the name aliases for a normalised sail number, or empty list if unknown.
         */
        List<String> boatAliases(String normalisedSailNumber)
        {
            return sailNo2Aliases.getOrDefault(normalisedSailNumber, List.of());
        }

        /**
         * Returns the canonical boat name for a normalised sail number, or null if not in seed.
         */
        String boatCanonicalName(String normalisedSailNumber)
        {
            return boatName2CanonicalName.get(normalisedSailNumber);
        }

        /**
         * Result of a boat seed lookup.
         */
        public record BoatMatch(String canonicalName, String canonicalSailNumber) {}

        /**
         * Looks up a boat entry by sail number or by normalised name.
         * Returns a BoatMatch if found, empty if not.
         */
        Optional<BoatMatch> lookupBoat(String normSailNumber, String normName)
        {
            BoatEntry e = sail2boat.get(normSailNumber);
            if (e == null)
                e = nameIndex.get(normName);
            if (e == null)
                return Optional.empty();
            // canonicalSailNumber is explicit in the entry; fall back to the queried sail number
            String canonicalSail = e.canonicalSailNumber != null
                ? IdGenerator.normaliseSailNumber(e.canonicalSailNumber)
                : normSailNumber;
            return Optional.of(new BoatMatch(e.canonicalName, canonicalSail));
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
        Yaml yaml = null;
        if (Files.exists(file))
        {
            try
            {
                yaml = YAML_MAPPER.readValue(file.toFile(), Yaml.class);
            }
            catch (Exception e)
            {
                LOG.error("Failed to read {} for update: {}", file, e.getMessage());
                return;
            }
        }
        if (yaml == null)
            yaml = new Yaml();
        if (yaml.designs == null)
            yaml.designs = new LinkedHashMap<>();

        DesignEntry entry = yaml.designs.computeIfAbsent(keepId, k -> new DesignEntry());
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
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), yaml);
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
        List<String> aliasNames     // names to record as aliases under sailNumber
    ) {}

    /**
     * Reads aliases.yaml, merges in entries from a merge operation (skipping existing entries),
     * and writes the file back.  Pre-existing comments in the file are not preserved.
     * <p>
     * Uses the new name-design key scheme: entries are keyed by normName[-designId].
     * Alternate sail numbers (merged-away boat's sail number when different from canonical) are
     * stored as alias entries with a {@code sailNumber} field.
     */
    public static void appendMergeAliases(Path configDir, List<MergeAliasSpec> specs)
    {
        Path file = configDir.resolve(FILENAME);
        Yaml yaml = null;
        if (Files.exists(file))
        {
            try
            {
                yaml = YAML_MAPPER.readValue(file.toFile(), Yaml.class);
            }
            catch (Exception e)
            {
                LOG.error("Failed to read {} for update: {}", file, e.getMessage());
                return;
            }
        }
        if (yaml == null)
            yaml = new Yaml();
        if (yaml.boats == null)
            yaml.boats = new LinkedHashMap<>();

        for (MergeAliasSpec spec : specs)
        {
            // YAML key is normSail-normName
            String yamlKey = IdGenerator.normaliseSailNumber(spec.canonicalSailNumber())
                + "-" + IdGenerator.normaliseName(spec.canonicalName());

            // Find or create entry
            BoatEntry entry = yaml.boats.computeIfAbsent(yamlKey, k ->
            {
                BoatEntry e = new BoatEntry();
                e.canonicalName = spec.canonicalName();
                e.canonicalSailNumber = spec.canonicalSailNumber();
                e.aliases = new ArrayList<>();
                return e;
            });
            if (entry.canonicalName == null)
                entry.canonicalName = spec.canonicalName();
            if (entry.canonicalSailNumber == null)
                entry.canonicalSailNumber = spec.canonicalSailNumber();
            if (entry.aliases == null)
                entry.aliases = new ArrayList<>();

            // Add name aliases
            for (String name : spec.aliasNames())
            {
                boolean present = entry.aliases.stream().anyMatch(a -> name.equalsIgnoreCase(a.name));
                if (!present)
                {
                    Alias ae = new Alias();
                    ae.name = name;
                    entry.aliases.add(ae);
                }
            }

            // Add alt sail number if the merged-away sail differs from the canonical
            if (!spec.sailNumber().equals(spec.canonicalSailNumber()))
            {
                String normAltSail = IdGenerator.normaliseSailNumber(spec.sailNumber());
                boolean altPresent = entry.aliases.stream()
                    .anyMatch(a -> a.sailNumber != null
                        && normAltSail.equalsIgnoreCase(IdGenerator.normaliseSailNumber(a.sailNumber)));
                if (!altPresent)
                {
                    Alias alt = new Alias();
                    alt.sailNumber = spec.sailNumber();
                    entry.aliases.add(alt);
                }

                // Remove the old sail-number-keyed entry if it exists (cleanup legacy entries)
                yaml.boats.remove(spec.sailNumber());
            }
        }

        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), yaml);
            LOG.info("Updated {} with {} merge alias spec(s)", file, specs.size());
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }

}
