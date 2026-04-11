package org.mortbay.sailing.hpf.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads {@code design.yaml} from the config directory (or classpath fallback) and returns
 * a {@link DesignCatalogue} that can answer whether a normalised design ID is excluded.
 */
class DesignLoader
{
    private static final Logger LOG = LoggerFactory.getLogger(DesignLoader.class);
    private static final String FILENAME = "design.yaml";
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
            .registerModule(new JavaTimeModule());

    static DesignCatalogue load(Path configDir)
    {
        InputStream stream = openStream(configDir, FILENAME);
        if (stream == null)
        {
            LOG.warn("No design.yaml found; design catalogue not loaded");
            return DesignCatalogue.EMPTY;
        }
        try
        {
            CatalogueFile file = YAML_MAPPER.readValue(stream, CatalogueFile.class);
            return new DesignCatalogue(file);
        }
        catch (Exception e)
        {
            LOG.error("Failed to load design.yaml: {}", e.getMessage(), e);
            return DesignCatalogue.EMPTY;
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
        return DesignLoader.class.getResourceAsStream("/" + filename);
    }

    // ---- YAML binding ----

    static class BoatOverrideEntry
    {
        public String sailNumber;
        public String name;
        public LocalDate from;
        public LocalDate until;
    }

    static class DesignOverride
    {
        public String designId;
        public String canonicalName;
        public List<BoatOverrideEntry> boats;
    }

    static class CatalogueFile
    {
        public List<String> excluded;
        public List<String> ignored;
        public List<DesignOverride> boatDesignOverrides;
    }

    // ---- Catalogue result ----

    /** A single design override entry with optional date range. */
    record OverrideEntry(String normDesignId, String canonicalName, LocalDate from, LocalDate until)
    {
        boolean isActiveOn(LocalDate date)
        {
            if (date == null)
                return from == null && until == null;
            if (from != null && date.isBefore(from))
                return false;
            if (until != null && date.isAfter(until))
                return false;
            return true;
        }
    }

    static class DesignCatalogue
    {
        static final DesignCatalogue EMPTY = new DesignCatalogue(null);

        private final Set<String> excludedIds;
        private final Set<String> ignoredIds;
        /** "normSail|normName" → list of override entries (possibly date-ranged) */
        private final Map<String, List<OverrideEntry>> overridesByKey;
        /** normDesignId → canonical name to use when creating the design (explicit canonicalName or raw designId) */
        private final Map<String, String> overrideDesigns;

        private DesignCatalogue(CatalogueFile file)
        {
            if (file == null)
            {
                excludedIds = Set.of();
                ignoredIds = Set.of();
                overridesByKey = Map.of();
                overrideDesigns = Map.of();
                return;
            }

            Set<String> ids = new HashSet<>();
            if (file.excluded != null)
            {
                for (String name : file.excluded)
                {
                    if (name != null && !name.isBlank())
                        ids.add(IdGenerator.normaliseDesignName(name));
                }
            }
            excludedIds = Collections.unmodifiableSet(ids);
            if (!ids.isEmpty())
                LOG.info("Loaded design catalogue: {} excluded design(s)", ids.size());

            Set<String> ign = new HashSet<>();
            if (file.ignored != null)
            {
                for (String name : file.ignored)
                {
                    if (name != null && !name.isBlank())
                        ign.add(IdGenerator.normaliseDesignName(name));
                }
            }
            ignoredIds = Collections.unmodifiableSet(ign);
            if (!ign.isEmpty())
                LOG.info("Loaded design catalogue: {} ignored design name(s)", ign.size());

            Map<String, List<OverrideEntry>> overrides = new HashMap<>();
            Map<String, String> designs = new HashMap<>();
            if (file.boatDesignOverrides != null)
            {
                for (DesignOverride override : file.boatDesignOverrides)
                {
                    if (override.designId == null || override.boats == null)
                        continue;
                    String normDesignId = IdGenerator.normaliseDesignName(override.designId);
                    String canonName = override.canonicalName != null && !override.canonicalName.isBlank()
                        ? override.canonicalName.trim() : override.designId;
                    designs.put(normDesignId, canonName);
                    for (BoatOverrideEntry boat : override.boats)
                    {
                        if (boat.sailNumber == null || boat.name == null)
                            continue;
                        String key = IdGenerator.normaliseSailNumber(boat.sailNumber)
                            + "|" + IdGenerator.normaliseName(boat.name);
                        overrides.computeIfAbsent(key, k -> new ArrayList<>())
                            .add(new OverrideEntry(normDesignId, canonName, boat.from, boat.until));
                    }
                }
            }
            overridesByKey  = Collections.unmodifiableMap(overrides);
            overrideDesigns = Collections.unmodifiableMap(designs);
            int overrideCount = overrides.values().stream().mapToInt(List::size).sum();
            if (overrideCount > 0)
                LOG.info("Loaded design catalogue: {} boat design override(s)", overrideCount);
        }

        boolean isExcluded(String normalisedDesignId)
        {
            if (normalisedDesignId == null)
                return false;
            return excludedIds.contains(normalisedDesignId);
        }

        boolean isIgnored(String normalisedDesignId)
        {
            if (normalisedDesignId == null)
                return false;
            return ignoredIds.contains(normalisedDesignId);
        }

        /**
         * Returns the normalised override designId for the given sail number, boat name, and optional date.
         * If date is null, only undated overrides match.
         */
        String resolveDesignOverride(String sailNumber, String name, LocalDate date)
        {
            OverrideEntry entry = findOverride(sailNumber, name, date);
            return entry != null ? entry.normDesignId() : null;
        }

        /**
         * Returns the canonical name for the given sail number and boat name's override,
         * or null if no override exists. Used when auto-creating a missing design.
         */
        String resolveRawDesignOverride(String sailNumber, String name, LocalDate date)
        {
            OverrideEntry entry = findOverride(sailNumber, name, date);
            return entry != null ? entry.canonicalName() : null;
        }

        private OverrideEntry findOverride(String sailNumber, String name, LocalDate date)
        {
            if (overridesByKey.isEmpty() || sailNumber == null || name == null)
                return null;
            String key = IdGenerator.normaliseSailNumber(sailNumber) + "|" + IdGenerator.normaliseName(name);
            List<OverrideEntry> entries = overridesByKey.get(key);
            if (entries == null)
                return null;
            // Prefer a date-specific match; fall back to undated
            OverrideEntry undated = null;
            for (OverrideEntry entry : entries)
            {
                if (entry.from() == null && entry.until() == null)
                    undated = entry;
                else if (entry.isActiveOn(date))
                    return entry;
            }
            return undated;
        }

        /**
         * Returns the normDesignId → canonical name map for all boatDesignOverride entries.
         * Used by DataStore to eagerly create designs on startup.
         */
        Map<String, String> overrideDesigns()
        {
            return overrideDesigns;
        }
    }
}
