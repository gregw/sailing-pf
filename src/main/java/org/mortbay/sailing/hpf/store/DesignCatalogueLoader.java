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
class DesignCatalogueLoader
{
    private static final Logger LOG = LoggerFactory.getLogger(DesignCatalogueLoader.class);
    private static final String FILENAME = "design.yaml";
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

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
        return DesignCatalogueLoader.class.getResourceAsStream("/" + filename);
    }

    // ---- YAML binding ----

    static class BoatOverrideEntry
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
        public List<DesignOverride> boatDesignOverrides;
    }

    // ---- Catalogue result ----

    static class DesignCatalogue
    {
        static final DesignCatalogue EMPTY = new DesignCatalogue(null);

        private final Set<String> excludedIds;
        private final Set<String> ignoredIds;
        /** "normSail|normName" → normalised designId */
        private final Map<String, String> overridesByKey;
        /** "normSail|normName" → raw designId as written in config (for canonical name on auto-create) */
        private final Map<String, String> rawOverridesByKey;
        /** normDesignId → canonical name to use when creating the design (explicit canonicalName or raw designId) */
        private final Map<String, String> overrideDesigns;

        private DesignCatalogue(CatalogueFile file)
        {
            if (file == null)
            {
                excludedIds = Set.of();
                ignoredIds = Set.of();
                overridesByKey = Map.of();
                rawOverridesByKey = Map.of();
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

            Map<String, String> overrides    = new HashMap<>();
            Map<String, String> rawOverrides = new HashMap<>();
            Map<String, String> designs      = new HashMap<>();
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
                        overrides.put(key, normDesignId);
                        rawOverrides.put(key, canonName);
                    }
                }
            }
            overridesByKey    = Collections.unmodifiableMap(overrides);
            rawOverridesByKey = Collections.unmodifiableMap(rawOverrides);
            overrideDesigns   = Collections.unmodifiableMap(designs);
            if (!overrides.isEmpty())
                LOG.info("Loaded design catalogue: {} boat design override(s)", overrides.size());
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
         * Returns the normalised override designId for the given sail number and boat name, or null if none.
         */
        String resolveDesignOverride(String sailNumber, String name)
        {
            if (overridesByKey.isEmpty() || sailNumber == null || name == null)
                return null;
            String key = IdGenerator.normaliseSailNumber(sailNumber) + "|" + IdGenerator.normaliseName(name);
            return overridesByKey.get(key);
        }

        /**
         * Returns the canonical name for the given sail number and boat name's override,
         * or null if no override exists. Used when auto-creating a missing design.
         */
        String resolveRawDesignOverride(String sailNumber, String name)
        {
            if (rawOverridesByKey.isEmpty() || sailNumber == null || name == null)
                return null;
            String key = IdGenerator.normaliseSailNumber(sailNumber) + "|" + IdGenerator.normaliseName(name);
            return rawOverridesByKey.get(key);
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
