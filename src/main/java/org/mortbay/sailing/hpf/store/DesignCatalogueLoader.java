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
        public List<BoatOverrideEntry> boats;
    }

    static class CatalogueFile
    {
        public List<String> excluded;
        public List<DesignOverride> boatDesignOverrides;
    }

    // ---- Catalogue result ----

    static class DesignCatalogue
    {
        static final DesignCatalogue EMPTY = new DesignCatalogue(null);

        private final Set<String> excludedIds;
        /** "normSail|normName" → designId */
        private final Map<String, String> overridesByKey;

        private DesignCatalogue(CatalogueFile file)
        {
            if (file == null)
            {
                excludedIds = Set.of();
                overridesByKey = Map.of();
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

            Map<String, String> overrides = new HashMap<>();
            if (file.boatDesignOverrides != null)
            {
                for (DesignOverride override : file.boatDesignOverrides)
                {
                    if (override.designId == null || override.boats == null)
                        continue;
                    String normDesignId = IdGenerator.normaliseDesignName(override.designId);
                    for (BoatOverrideEntry boat : override.boats)
                    {
                        if (boat.sailNumber == null || boat.name == null)
                            continue;
                        String key = IdGenerator.normaliseSailNumber(boat.sailNumber)
                            + "|" + IdGenerator.normaliseName(boat.name);
                        overrides.put(key, normDesignId);
                    }
                }
            }
            overridesByKey = Collections.unmodifiableMap(overrides);
            if (!overrides.isEmpty())
                LOG.info("Loaded design catalogue: {} boat design override(s)", overrides.size());
        }

        boolean isExcluded(String normalisedDesignId)
        {
            if (normalisedDesignId == null)
                return false;
            return excludedIds.contains(normalisedDesignId);
        }

        /**
         * Returns the override designId for the given sail number and boat name, or null if none.
         */
        String resolveDesignOverride(String sailNumber, String name)
        {
            if (overridesByKey.isEmpty() || sailNumber == null || name == null)
                return null;
            String key = IdGenerator.normaliseSailNumber(sailNumber) + "|" + IdGenerator.normaliseName(name);
            return overridesByKey.get(key);
        }
    }
}
