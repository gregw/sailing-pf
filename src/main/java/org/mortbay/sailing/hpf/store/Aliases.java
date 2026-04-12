package org.mortbay.sailing.hpf.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.eclipse.jetty.util.StringUtil;
import org.mortbay.sailing.hpf.importer.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Reads {@code /aliases.yaml} from the classpath and provides lookup methods
 * for design and boat name aliases.
 * <p>
 * The loaded seed is lookup-only and is never written back to disk.
 */
public class Aliases
{
    private static final Logger LOG = LoggerFactory.getLogger(org.mortbay.sailing.hpf.store.Aliases.class);
    private static final String FILENAME = "aliases.yaml";
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

    /** Australian country/fleet sail-number prefixes. "AUS5656" and "5656" are the same boat. */
    private static final List<String> AUS_PREFIXES = List.of("JAUS", "EAUS", "VAUS", "SAUS", "AUS");

    /**
     * Strip a known Australian country/fleet prefix from a normalised sail number.
     * "AUS5656" → "5656", "JAUS103" → "103", "5656" → "5656".
     * Only strips when the prefix is immediately followed by a digit.
     * "JAUS" is listed before "AUS" so that "JAUS103" → "103", not "US103".
     */
    private static String stripPrefix(String normSail)
    {
        for (String prefix : AUS_PREFIXES)
        {
            if (normSail.startsWith(prefix) && normSail.length() > prefix.length()
                && Character.isDigit(normSail.charAt(prefix.length())))
                return normSail.substring(prefix.length());
        }
        return normSail;
    }

    static Loaded load(Path configDir)
    {
        InputStream stream = openStream(configDir, FILENAME);
        if (stream == null)
        {
            LOG.warn("No aliases.yaml found; alias seed not loaded");
            return Loaded.EMPTY;
        }
        try
        {
            Yaml yaml = YAML_MAPPER.readValue(stream, Yaml.class);
            return new Loaded(yaml);
        }
        catch (Exception e)
        {
            LOG.error("Failed to load aliases.yaml: {}", e.getMessage(), e);
            return Loaded.EMPTY;
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
        return org.mortbay.sailing.hpf.store.Aliases.class.getResourceAsStream("/" + filename);
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

    public record BoatEntry (String canonicalName, List<SailNumberName> aliases){};

    public record SailNumberName(String sailNumber, String name)
    {}

    /**
     * Loaded alias data. Immutable after construction; all lookups are O(1).
     */
    static class Loaded
    {
        static final Loaded EMPTY = new Loaded(null);

        /** normAlias → canonical design ID */
        private final Map<String, String> name2designId;
        /** canonical design ID → canonical display name */
        private final Map<String, String> designId2Name;

        private final Map<String, List<BoatEntry>> sailNo2Aliases;
        private final Map<String, List<BoatEntry>> name2Aliases;

        private Loaded(Yaml yaml)
        {
            if (yaml == null || (yaml.designs == null && yaml.boats == null))
            {
                name2designId = Map.of();
                designId2Name = Map.of();
                sailNo2Aliases = Map.of();
                name2Aliases = Map.of();
                return;
            }

            // Build design indexes
            Map<String, String> name2designIdMutable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            Map<String, String> designId2NameMutable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            if (yaml.designs != null)
            {
                for (Map.Entry<String, DesignEntry> e : yaml.designs.entrySet())
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
            Map<String, List<BoatEntry>> sailNo2AliasesMutable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            Map<String, List<BoatEntry>> name2AliasesMutable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            if (yaml.boats != null && !yaml.boats.isEmpty())
            {
                for (Map.Entry<String, BoatEntry> e : yaml.boats.entrySet())
                {
                    String sailNumName = e.getKey();
                    BoatEntry entry = e.getValue();

                    // Key format: normSail-normName (e.g. "MYC7-daydreaming")
                    int dash = sailNumName.indexOf('-');
                    if (dash < 0)
                    {
                        LOG.warn("Skipping boat alias entry with invalid key (no dash): {}", sailNumName);
                        continue;
                    }
                    String canonicalSail = sailNumName.substring(0, dash);
                    String normName = sailNumName.substring(dash + 1);

                    if (!IdGenerator.normaliseName(e.getValue().canonicalName).equalsIgnoreCase(normName))
                    {
                        LOG.warn("Skipping boat alias entry with invalid canonical name: {} {}",
                            sailNumName, e.getValue().canonicalName);
                        continue;
                    }

                    List<SailNumberName> aliases = new ArrayList<>();
                    for (SailNumberName snn : e.getValue().aliases)
                    {
                        if (snn.sailNumber != null && !snn.sailNumber.isBlank())
                        {
                            if (snn.name != null && !snn.name.isBlank())
                                aliases.add(new SailNumberName(IdGenerator.normaliseSailNumber(snn.sailNumber), IdGenerator.normaliseName(snn.name)));
                            else
                                aliases.add(new SailNumberName(IdGenerator.normaliseSailNumber(snn.sailNumber), normName));
                        }
                        else if (snn.name != null && !snn.name.isBlank())
                        {
                            aliases.add(new SailNumberName(canonicalSail, IdGenerator.normaliseName(snn.name)));
                        }
                    }

                    final BoatEntry aliased = new BoatEntry(entry.canonicalName, aliases);

                    for (SailNumberName snn : aliases)
                    {
                        if (!canonicalSail.equalsIgnoreCase(snn.sailNumber))
                        {
                            sailNo2AliasesMutable.compute(snn.sailNumber, (k, v) ->
                            {
                                if (v == null || v.isEmpty())
                                    return List.of(aliased);
                                List<BoatEntry> list = new ArrayList<>(v);
                                list.add(aliased);
                                return Collections.unmodifiableList(list);
                            });
                        }
                        if (!normName.equalsIgnoreCase(snn.name))
                        {
                            name2AliasesMutable.compute(snn.name, (k, v) ->
                            {
                                if (v == null || v.isEmpty())
                                    return List.of(aliased);
                                List<BoatEntry> list = new ArrayList<>(v);
                                list.add(aliased);
                                return Collections.unmodifiableList(list);
                            });
                        }
                    }
                }
            }
            sailNo2Aliases = Collections.unmodifiableMap(sailNo2AliasesMutable);
            name2Aliases = Collections.unmodifiableMap(name2AliasesMutable);
            LOG.debug("Boat alias sailNumberIndex keys: {}", sailNo2Aliases);
            LOG.debug("Boat alias nameIndex keys: {}", name2Aliases);
        }

        /**
         * Resolves a normalised design alias to a canonical design ID, or null if unknown.
         */
        String resolveDesignAlias(String normalisedAlias)
        {
            if (StringUtil.isBlank(normalisedAlias))
                return normalisedAlias;
            return Objects.requireNonNullElse(name2designId.get(normalisedAlias), normalisedAlias);
        }

        /**
         * Returns the canonical display name for a design ID, or null if not in seed.
         */
        String designCanonicalName(String designId)
        {
            return designId2Name.get(designId);
        }

        /**
         * Looks up a boat entry by sail number or by normalised name.
         * Returns a BoatMatch if found, empty if not.
         */
        Optional<SailNumberName> lookupBoat(String normSailNumber, String normName)
        {
            // Look up sail number first then check the names
            List<BoatEntry> entries = sailNo2Aliases.get(normSailNumber);
            if (entries == null || entries.isEmpty())
                entries = sailNo2Aliases.get(stripPrefix(normSailNumber));
            if (entries != null && !entries.isEmpty())
            {
                for (BoatEntry boat : entries)
                {
                    for (SailNumberName alias : boat.aliases)
                    {
                        if (alias.name != null && alias.name.equalsIgnoreCase(normName))
                            return Optional.of(alias);
                    }
                }
            }

            // Look up name and then check the sail number.
            entries = name2Aliases.get(normName);
            if (entries != null && !entries.isEmpty())
            {
                for (BoatEntry boat : entries)
                {
                    for (SailNumberName alias : boat.aliases)
                    {
                        if (alias.sailNumber != null && alias.sailNumber.equalsIgnoreCase(normSailNumber))
                            return Optional.of(alias);
                    }
                }
            }

            return Optional.empty();
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
     * Reads aliases.yaml, merges in entries from a merge operation (skipping existing entries),
     * and writes the file back.  Pre-existing comments in the file are not preserved.
     * <p>
     * Uses the new name-design key scheme: entries are keyed by normName[-designId].
     * Alternate sail numbers (merged-away boat's sail number when different from canonical) are
     * stored as alias entries with a {@code sailNumber} field.
     */
    public static void addAliases(Path configDir, String normSailNo, String canonicalName, List<SailNumberName> aliases)
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

        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), yaml);
            LOG.info("Updated {} with {} merge alias spec(s)", file, aliases.size());
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }

}
