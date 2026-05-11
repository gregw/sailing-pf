package org.mortbay.sailing.pf.store;

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
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.mortbay.sailing.pf.importer.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@code design.yaml} from the config directory (or classpath fallback) and returns
 * a {@link DesignCatalogue} that can answer whether a normalised design ID is excluded.
 */
class Designs
{
    private static final Logger LOG = LoggerFactory.getLogger(Designs.class);
    private static final String FILENAME = "design.yaml";
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
            .registerModule(new JavaTimeModule());

    /**
     * Adds or updates a boat design override in {@code design.yaml}.
     * Finds the existing {@code DesignOverride} block for {@code designId} (or creates one),
     * then adds a {@code BoatOverrideEntry} for the given sail number and name if not already present.
     */
    static void addDesignOverride(Path configDir, String sailNumber, String name,
                                  String designId, String canonicalName)
    {
        Path file = configDir.resolve(FILENAME);
        CatalogueFile catalogue = null;
        if (Files.exists(file))
        {
            try
            {
                catalogue = YAML_MAPPER.readValue(file.toFile(), CatalogueFile.class);
            }
            catch (Exception e)
            {
                LOG.error("Failed to read {} for update: {}", file, e.getMessage());
                return;
            }
        }
        if (catalogue == null)
            catalogue = new CatalogueFile();
        if (catalogue.boatDesignOverrides == null)
            catalogue.boatDesignOverrides = new ArrayList<>();

        // Find or create the DesignOverride block for this designId
        DesignOverride block = catalogue.boatDesignOverrides.stream()
            .filter(o -> Objects.equals(o.designId, designId))
            .findFirst().orElse(null);
        if (block == null)
        {
            block = new DesignOverride();
            block.designId = designId;
            block.canonicalName = canonicalName;
            block.boats = new ArrayList<>();
            catalogue.boatDesignOverrides.add(block);
        }
        if (block.boats == null)
            block.boats = new ArrayList<>();

        // Add the boat entry if not already present (match on sailNumber + name)
        String normSail = IdGenerator.normaliseSailNumber(sailNumber);
        String normName = IdGenerator.normaliseName(name);
        boolean duplicate = block.boats.stream().anyMatch(b ->
            Objects.equals(IdGenerator.normaliseSailNumber(b.sailNumber), normSail)
            && Objects.equals(IdGenerator.normaliseName(b.name), normName));
        if (!duplicate)
        {
            BoatOverrideEntry entry = new BoatOverrideEntry();
            entry.sailNumber = sailNumber;
            entry.name = name;
            block.boats.add(entry);
        }

        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), catalogue);
            LOG.info("Updated {} with design override {} for {}/{}", file, designId, sailNumber, name);
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }

    /** Enumerates the toggleable design-catalogue flag fields in {@code design.yaml}. */
    enum Flag
    {EXCLUDED, IGNORED, NO_SPINNAKER}

    /**
     * Reads {@code design.yaml}, toggles {@code designId} on or off the specified flag list,
     * and writes the file back. This is the canonical persistence path for design-level
     * excluded/ignored state; runtime state in {@link DataStore} is kept in sync via
     * {@link DataStore#reloadDesignCatalogue()} after a write.
     */
    static void setFlag(Path configDir, String designId, Flag flag, boolean set)
    {
        if (designId == null || designId.isBlank()) return;
        Path file = configDir.resolve(FILENAME);
        CatalogueFile catalogue = null;
        if (Files.exists(file))
        {
            try { catalogue = YAML_MAPPER.readValue(file.toFile(), CatalogueFile.class); }
            catch (Exception e)
            {
                LOG.error("Failed to read {} for flag update: {}", file, e.getMessage());
                return;
            }
        }
        if (catalogue == null) catalogue = new CatalogueFile();
        List<String> list = switch (flag)
        {
            case EXCLUDED -> catalogue.excluded;
            case IGNORED -> catalogue.ignored;
            case NO_SPINNAKER -> catalogue.noSpinnaker;
        };
        if (list == null)
        {
            list = new ArrayList<>();
            switch (flag)
            {
                case EXCLUDED -> catalogue.excluded = list;
                case IGNORED -> catalogue.ignored = list;
                case NO_SPINNAKER -> catalogue.noSpinnaker = list;
            }
        }
        // Compare by normalised id so "Foo 36", "foo36", "Foo-36" match a single entry.
        String normTarget = IdGenerator.normaliseDesignName(designId);
        boolean present = list.stream()
            .anyMatch(x -> x != null && IdGenerator.normaliseDesignName(x).equalsIgnoreCase(normTarget));
        if (set && !present)
            list.add(designId);
        else if (!set && present)
            list.removeIf(x -> x != null && IdGenerator.normaliseDesignName(x).equalsIgnoreCase(normTarget));
        else
            return;  // already in the desired state

        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), catalogue);
            LOG.info("Updated {} — design {} {} {}",
                file, designId, flag.name().toLowerCase(), set ? "set" : "cleared");
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }

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
        return Designs.class.getResourceAsStream("/" + filename);
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
        public List<String> noSpinnaker;
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
        private final Set<String> noSpinnakerIds;
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
                noSpinnakerIds = Set.of();
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

            Set<String> nspin = new HashSet<>();
            if (file.noSpinnaker != null)
            {
                for (String name : file.noSpinnaker)
                {
                    if (name != null && !name.isBlank())
                        nspin.add(IdGenerator.normaliseDesignName(name));
                }
            }
            noSpinnakerIds = Collections.unmodifiableSet(nspin);
            if (!nspin.isEmpty())
                LOG.info("Loaded design catalogue: {} no-spinnaker design(s)", nspin.size());

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

        boolean isNoSpinnaker(String normalisedDesignId)
        {
            if (normalisedDesignId == null)
                return false;
            return noSpinnakerIds.contains(normalisedDesignId);
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
