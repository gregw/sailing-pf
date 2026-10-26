package org.mortbay.sailing.pf.store;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
    {EXCLUDED, IGNORED, NO_SPINNAKER, ONE_DESIGN}

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
            case ONE_DESIGN -> catalogue.oneDesign;
        };
        if (list == null)
        {
            list = new ArrayList<>();
            switch (flag)
            {
                case EXCLUDED -> catalogue.excluded = list;
                case IGNORED -> catalogue.ignored = list;
                case NO_SPINNAKER -> catalogue.noSpinnaker = list;
                case ONE_DESIGN -> catalogue.oneDesign = list;
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

    /**
     * Reads {@code design.yaml}, adds or removes the boat (by sail number + name) on the
     * {@code modifiedBoats:} list, and writes the file back. Keyed by sail + name rather than
     * boat id so the flag survives a design change (which re-keys the boat id).
     */
    static void setModifiedBoat(Path configDir, String sailNumber, String name, boolean set)
    {
        if (sailNumber == null || name == null) return;
        Path file = configDir.resolve(FILENAME);
        CatalogueFile catalogue = null;
        if (Files.exists(file))
        {
            try { catalogue = YAML_MAPPER.readValue(file.toFile(), CatalogueFile.class); }
            catch (Exception e)
            {
                LOG.error("Failed to read {} for modified-boat update: {}", file, e.getMessage());
                return;
            }
        }
        if (catalogue == null) catalogue = new CatalogueFile();
        if (catalogue.modifiedBoats == null) catalogue.modifiedBoats = new ArrayList<>();

        String key = boatKey(sailNumber, name);
        boolean present = catalogue.modifiedBoats.stream().anyMatch(b -> key.equals(boatKey(b.sailNumber, b.name)));
        if (set && !present)
        {
            BoatEntry entry = new BoatEntry();
            entry.sailNumber = sailNumber;
            entry.name = name;
            catalogue.modifiedBoats.add(entry);
        }
        else if (!set && present)
            catalogue.modifiedBoats.removeIf(b -> key.equals(boatKey(b.sailNumber, b.name)));
        else
            return;  // already in the desired state

        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), catalogue);
            LOG.info("Updated {} — boat {}/{} modified {}", file, sailNumber, name, set ? "set" : "cleared");
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }

    /**
     * Moves every boat design override targeting {@code fromDesignId} to {@code toDesignId}
     * (merging into an existing block for it), and writes the file back if anything changed.
     *
     * @return true if design.yaml was changed
     */
    static boolean retargetDesignOverrides(Path configDir, String fromDesignId, String toDesignId,
                                           String toCanonicalName)
    {
        Path file = configDir.resolve(FILENAME);
        if (!Files.exists(file))
            return false;
        CatalogueFile catalogue;
        try { catalogue = YAML_MAPPER.readValue(file.toFile(), CatalogueFile.class); }
        catch (Exception e)
        {
            LOG.error("Failed to read {} for override retarget: {}", file, e.getMessage());
            return false;
        }
        if (catalogue == null || catalogue.boatDesignOverrides == null)
            return false;
        String from = IdGenerator.normaliseDesignName(fromDesignId);
        List<DesignOverride> moving = catalogue.boatDesignOverrides.stream()
            .filter(o -> o.designId != null && IdGenerator.normaliseDesignName(o.designId).equals(from))
            .toList();
        if (moving.isEmpty())
            return false;

        catalogue.boatDesignOverrides.removeAll(moving);
        DesignOverride target = catalogue.boatDesignOverrides.stream()
            .filter(o -> Objects.equals(o.designId, toDesignId))
            .findFirst().orElse(null);
        if (target == null)
        {
            target = new DesignOverride();
            target.designId = toDesignId;
            target.canonicalName = toCanonicalName;
            target.boats = new ArrayList<>();
            catalogue.boatDesignOverrides.add(target);
        }
        if (target.boats == null)
            target.boats = new ArrayList<>();
        for (DesignOverride o : moving)
        {
            if (o.boats == null)
                continue;
            for (BoatOverrideEntry b : o.boats)
            {
                String key = boatKey(b.sailNumber, b.name);
                boolean dup = target.boats.stream().anyMatch(t -> Objects.equals(key, boatKey(t.sailNumber, t.name))
                    && Objects.equals(t.from, b.from) && Objects.equals(t.until, b.until));
                if (!dup)
                    target.boats.add(b);
            }
        }

        try
        {
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), catalogue);
            LOG.info("Updated {} — design overrides for {} moved to {}", file, fromDesignId, toDesignId);
            return true;
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
            return false;
        }
    }

    /**
     * Reads {@code design.yaml}, sets (or with {@code year} null, removes) the design's manual
     * year in {@code designYears:}, and writes the file back.
     */
    static void setDesignYear(Path configDir, String designId, Integer year)
    {
        if (designId == null || designId.isBlank()) return;
        Path file = configDir.resolve(FILENAME);
        CatalogueFile catalogue = null;
        if (Files.exists(file))
        {
            try { catalogue = YAML_MAPPER.readValue(file.toFile(), CatalogueFile.class); }
            catch (Exception e)
            {
                LOG.error("Failed to read {} for design year update: {}", file, e.getMessage());
                return;
            }
        }
        if (catalogue == null) catalogue = new CatalogueFile();
        if (catalogue.designYears == null) catalogue.designYears = new LinkedHashMap<>();
        String norm = IdGenerator.normaliseDesignName(designId);
        catalogue.designYears.keySet().removeIf(k -> IdGenerator.normaliseDesignName(k).equals(norm));
        if (year != null)
            catalogue.designYears.put(norm, year);

        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), catalogue);
            LOG.info("Updated {} — design {} year {}", file, designId, year);
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }

    /** "normSail|normName" key, as used for boat design overrides and modified boats. */
    static String boatKey(String sailNumber, String name)
    {
        if (sailNumber == null || name == null)
            return null;
        return IdGenerator.normaliseSailNumber(sailNumber) + "|" + IdGenerator.normaliseName(name);
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

    /** A boat named by sail number + name (the {@code modifiedBoats:} list). */
    static class BoatEntry
    {
        public String sailNumber;
        public String name;
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
        public List<String> oneDesign;
        /** Manual design year (the design's first year), overriding the ORC age dates. */
        public Map<String, Integer> designYears;
        public List<BoatEntry> modifiedBoats;
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
        private final Set<String> oneDesignIds;
        /** "normSail|normName" of boats ticked as modified from their design */
        private final Set<String> modifiedBoatKeys;
        /** normDesignId → manual design year */
        private final Map<String, Integer> designYears;
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
                oneDesignIds = Set.of();
                modifiedBoatKeys = Set.of();
                designYears = Map.of();
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

            Set<String> od = new HashSet<>();
            if (file.oneDesign != null)
            {
                for (String name : file.oneDesign)
                {
                    if (name != null && !name.isBlank())
                        od.add(IdGenerator.normaliseDesignName(name));
                }
            }
            oneDesignIds = Collections.unmodifiableSet(od);
            if (!od.isEmpty())
                LOG.info("Loaded design catalogue: {} one-design design(s)", od.size());

            Set<String> mod = new HashSet<>();
            if (file.modifiedBoats != null)
            {
                for (BoatEntry b : file.modifiedBoats)
                {
                    String key = b == null ? null : boatKey(b.sailNumber, b.name);
                    if (key != null)
                        mod.add(key);
                }
            }
            modifiedBoatKeys = Collections.unmodifiableSet(mod);
            if (!mod.isEmpty())
                LOG.info("Loaded design catalogue: {} modified boat(s)", mod.size());

            Map<String, Integer> years = new HashMap<>();
            if (file.designYears != null)
                file.designYears.forEach((id, y) ->
                {
                    if (id != null && y != null)
                        years.put(IdGenerator.normaliseDesignName(id), y);
                });
            designYears = Collections.unmodifiableMap(years);

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
                        String key = boatKey(boat.sailNumber, boat.name);
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
         * Returns the normalised design ids currently flagged as ignored. The startup
         * repair in {@link DataStore} iterates this to cascade-merge orphan boats whose
         * stored designId is on the ignored list but whose record was created before
         * the design was ignored (so {@link DataStore#cascadeIgnoreDesign} never fired
         * for them).
         */
        Set<String> ignoredDesignIds()
        {
            return ignoredIds;
        }

        boolean isNoSpinnaker(String normalisedDesignId)
        {
            if (normalisedDesignId == null)
                return false;
            return noSpinnakerIds.contains(normalisedDesignId);
        }

        boolean isOneDesign(String normalisedDesignId)
        {
            if (normalisedDesignId == null)
                return false;
            return oneDesignIds.contains(normalisedDesignId);
        }

        /** The design's manual year, or null if none is set. */
        Integer designYear(String normalisedDesignId)
        {
            return normalisedDesignId == null ? null : designYears.get(normalisedDesignId);
        }

        boolean isModifiedBoat(String sailNumber, String name)
        {
            String key = boatKey(sailNumber, name);
            return key != null && modifiedBoatKeys.contains(key);
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
            String key = boatKey(sailNumber, name);
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
