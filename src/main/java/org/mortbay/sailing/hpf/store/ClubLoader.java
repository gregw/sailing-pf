package org.mortbay.sailing.hpf.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.mortbay.sailing.hpf.data.Club;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads {@code /clubs.yaml} from the classpath and returns stub {@link Club} records
 * for each entry that has a real (non-placeholder) domain.
 * <p>
 * Entries whose domain key starts with {@code "unknown.domain."} are skipped with a
 * warning — they are placeholders pending manual completion by the user.
 */
class ClubLoader
{
    private static final Logger LOG = LoggerFactory.getLogger(ClubLoader.class);
    private static final String FILENAME = "clubs.yaml";
    private static final String PLACEHOLDER_PREFIX = "unknown.domain.";
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    static Map<String, Club> load(Path configDir)
    {
        InputStream stream = openStream(configDir, FILENAME);
        if (stream == null)
        {
            LOG.warn("No clubs.yaml found; club seed not loaded");
            return Map.of();
        }
        try
        {
            SeedFile seedFile = YAML_MAPPER.readValue(stream, SeedFile.class);
            Map<String, Club> result = new LinkedHashMap<>();
            if (seedFile.clubs == null)
                return result;
            for (Map.Entry<String, SeedEntry> e : seedFile.clubs.entrySet())
            {
                String domain = e.getKey();
                SeedEntry entry = e.getValue();
                if (domain.startsWith(PLACEHOLDER_PREFIX))
                {
                    LOG.warn("Skipping club seed entry with placeholder domain: {}", domain);
                    continue;
                }
                Club stub = new Club(domain, entry.shortName, entry.fullName, entry.state,
                    Boolean.TRUE.equals(entry.excluded),
                    entry.aliases != null ? entry.aliases : List.of(),
                    entry.topyacht != null ? entry.topyacht : List.of(),
                    List.of(), null);
                result.put(domain, stub);
            }
            LOG.info("Loaded {} club seed entries from {}", result.size(), FILENAME);
            return result;
        }
        catch (Exception e)
        {
            LOG.error("Failed to load clubs.yaml: {}", e.getMessage(), e);
            return Map.of();
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
        return ClubLoader.class.getResourceAsStream("/" + filename);
    }

    static class SeedFile
    {
        public Map<String, SeedEntry> clubs;
    }

    static class SeedEntry
    {
        public String shortName;
        public String state;
        public String fullName;
        public Boolean excluded;
        public List<String> aliases;
        public List<String> topyacht;
    }
}
