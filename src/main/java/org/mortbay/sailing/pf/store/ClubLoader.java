package org.mortbay.sailing.pf.store;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.importer.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
        new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

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
                    Boolean.TRUE.equals(entry.excluded), entry.email,
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

    static ClubCatalogue loadCatalogue(Path configDir)
    {
        InputStream stream = openStream(configDir, FILENAME);
        if (stream == null)
        {
            LOG.warn("No clubs.yaml found; club catalogue not loaded");
            return ClubCatalogue.EMPTY;
        }
        try
        {
            SeedFile seedFile = YAML_MAPPER.readValue(stream, SeedFile.class);
            return new ClubCatalogue(seedFile);
        }
        catch (Exception e)
        {
            LOG.error("Failed to load clubs.yaml for catalogue: {}", e.getMessage(), e);
            return ClubCatalogue.EMPTY;
        }
    }

    /**
     * Adds or updates a boat club override in {@code clubs.yaml}.
     * Appends a {@code ClubOverride} entry for the given sail number and name if not already present.
     */
    static void addClubOverride(Path configDir, String sailNumber, String name, String clubId)
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
        if (seedFile.boatClubOverrides == null)
            seedFile.boatClubOverrides = new ArrayList<>();

        String normSail = IdGenerator.normaliseSailNumber(sailNumber);
        String normName = IdGenerator.normaliseName(name);
        boolean duplicate = seedFile.boatClubOverrides.stream().anyMatch(o ->
            Objects.equals(IdGenerator.normaliseSailNumber(o.sailNumber), normSail)
            && Objects.equals(IdGenerator.normaliseName(o.name), normName));
        if (!duplicate)
        {
            ClubOverride entry = new ClubOverride();
            entry.sailNumber = sailNumber;
            entry.name = name;
            entry.clubId = clubId;
            seedFile.boatClubOverrides.add(entry);
        }
        else
        {
            // Update existing entry's clubId
            for (ClubOverride o : seedFile.boatClubOverrides)
            {
                if (Objects.equals(IdGenerator.normaliseSailNumber(o.sailNumber), normSail)
                    && Objects.equals(IdGenerator.normaliseName(o.name), normName))
                {
                    o.clubId = clubId;
                    break;
                }
            }
        }

        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), seedFile);
            LOG.info("Updated {} with club override {} for {}/{}", file, clubId, sailNumber, name);
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }

    /**
     * Assigns a boatId to a specific club in clubs.yaml (per-club {@code boats} list).
     * Removes the boatId from {@code noclub} and from any other club's {@code boats} list.
     */
    static void setBoatClub(Path configDir, String boatId, String clubId)
    {
        SeedFile seedFile = readOrNew(configDir);
        if (seedFile == null)
            return;

        // Remove from noclub
        if (seedFile.noclub != null)
            seedFile.noclub.remove(boatId);

        // Remove from any other club's boats list, and ensure target club has the entry
        if (seedFile.clubs != null)
        {
            for (Map.Entry<String, SeedEntry> e : seedFile.clubs.entrySet())
            {
                SeedEntry entry = e.getValue();
                if (entry.boats != null)
                    entry.boats.remove(boatId);
                if (e.getKey().equals(clubId))
                {
                    if (entry.boats == null)
                        entry.boats = new ArrayList<>();
                    if (!entry.boats.contains(boatId))
                        entry.boats.add(boatId);
                }
            }
        }

        writeOrLog(configDir, seedFile);
        LOG.info("clubs.yaml: boatId {} assigned to club {}", boatId, clubId);
    }

    /**
     * Assigns a boatId to a list of clubs in clubs.yaml (per-club {@code boats} list).
     * Removes the boatId from {@code noclub} and from any other club's {@code boats} list
     * not in {@code clubIds}; appends the boatId to each listed club's {@code boats}.
     * <p>
     * If {@code clubIds} is empty, the boatId is moved to the {@code noclub} list.
     */
    static void setBoatClubs(Path configDir, String boatId, List<String> clubIds)
    {
        if (clubIds == null || clubIds.isEmpty())
        {
            setBoatNoClub(configDir, boatId);
            return;
        }

        SeedFile seedFile = readOrNew(configDir);
        if (seedFile == null)
            return;

        // Remove from noclub
        if (seedFile.noclub != null)
            seedFile.noclub.remove(boatId);

        Set<String> wanted = new LinkedHashSet<>(clubIds);

        // Strip the entry from any club not in `wanted`, and ensure each wanted club has it
        if (seedFile.clubs == null)
            seedFile.clubs = new LinkedHashMap<>();
        for (Map.Entry<String, SeedEntry> e : seedFile.clubs.entrySet())
        {
            SeedEntry entry = e.getValue();
            if (entry.boats == null)
                continue;
            if (!wanted.contains(e.getKey()))
                entry.boats.remove(boatId);
        }
        for (String cid : wanted)
        {
            SeedEntry entry = seedFile.clubs.get(cid);
            if (entry == null)
            {
                entry = new SeedEntry();
                seedFile.clubs.put(cid, entry);
            }
            if (entry.boats == null)
                entry.boats = new ArrayList<>();
            if (!entry.boats.contains(boatId))
                entry.boats.add(boatId);
        }

        writeOrLog(configDir, seedFile);
        LOG.info("clubs.yaml: boatId {} assigned to clubs {}", boatId, wanted);
    }

    /**
     * Marks a boatId as having no club in clubs.yaml (adds to {@code noclub} list).
     * Removes the boatId from all per-club {@code boats} lists.
     */
    static void setBoatNoClub(Path configDir, String boatId)
    {
        SeedFile seedFile = readOrNew(configDir);
        if (seedFile == null)
            return;

        // Add to noclub if not already there
        if (seedFile.noclub == null)
            seedFile.noclub = new ArrayList<>();
        if (!seedFile.noclub.contains(boatId))
            seedFile.noclub.add(boatId);

        // Remove from all clubs' boats lists
        if (seedFile.clubs != null)
            for (SeedEntry entry : seedFile.clubs.values())
            {
                if (entry.boats != null)
                    entry.boats.remove(boatId);
            }

        writeOrLog(configDir, seedFile);
        LOG.info("clubs.yaml: boatId {} marked as no-club", boatId);
    }

    /**
     * Renames a boatId in clubs.yaml — updates {@code noclub} and all per-club {@code boats} lists.
     */
    static void remapBoatId(Path configDir, String oldBoatId, String newBoatId)
    {
        if (Objects.equals(oldBoatId, newBoatId))
            return;
        SeedFile seedFile = readOrNew(configDir);
        if (seedFile == null)
            return;

        boolean changed = false;

        if (seedFile.noclub != null)
        {
            int idx = seedFile.noclub.indexOf(oldBoatId);
            if (idx >= 0 && !seedFile.noclub.contains(newBoatId))
            {
                seedFile.noclub.set(idx, newBoatId);
                changed = true;
            }
            else if (idx >= 0)
            {
                seedFile.noclub.remove(idx);
                changed = true;
            }
        }

        if (seedFile.clubs != null)
        {
            for (SeedEntry entry : seedFile.clubs.values())
            {
                if (entry.boats == null)
                    continue;
                int idx = entry.boats.indexOf(oldBoatId);
                if (idx >= 0 && !entry.boats.contains(newBoatId))
                {
                    entry.boats.set(idx, newBoatId);
                    changed = true;
                }
                else if (idx >= 0)
                {
                    entry.boats.remove(idx);
                    changed = true;
                }
            }
        }

        if (changed)
        {
            writeOrLog(configDir, seedFile);
            LOG.info("clubs.yaml: remapped boatId {} → {}", oldBoatId, newBoatId);
        }
    }

    /**
     * Sets the {@code excluded} flag for a club in clubs.yaml. If the club has no entry yet,
     * one is auto-created using {@code shortNameIfNew}. Returns true if the file was changed.
     */
    static boolean setClubExcluded(Path configDir, String clubId,
                                   String shortNameIfNew, boolean excluded)
    {
        SeedFile seedFile = readOrNew(configDir);
        if (seedFile == null)
            return false;
        if (seedFile.clubs == null)
            seedFile.clubs = new LinkedHashMap<>();

        SeedEntry entry = seedFile.clubs.get(clubId);
        boolean created = false;
        if (entry == null)
        {
            entry = new SeedEntry();
            entry.shortName = shortNameIfNew;
            seedFile.clubs.put(clubId, entry);
            created = true;
        }

        Boolean newValue = excluded ? Boolean.TRUE : null;
        if (!created && Objects.equals(entry.excluded, newValue))
            return false;

        entry.excluded = newValue;
        writeOrLog(configDir, seedFile);
        LOG.info("clubs.yaml: club {} excluded={}", clubId, excluded);
        return true;
    }

    /**
     * Updates {@code fullName}, {@code state}, and {@code email} for a club in clubs.yaml.
     * Each value is written verbatim (including null, which clears the field). If the club
     * has no entry yet, one is auto-created using {@code shortNameIfNew}. Returns true if
     * the file was changed.
     */
    static boolean updateClubMeta(Path configDir, String clubId, String shortNameIfNew,
                                  String longName, String state, String email)
    {
        SeedFile seedFile = readOrNew(configDir);
        if (seedFile == null)
            return false;
        if (seedFile.clubs == null)
            seedFile.clubs = new LinkedHashMap<>();

        SeedEntry entry = seedFile.clubs.get(clubId);
        boolean created = false;
        if (entry == null)
        {
            entry = new SeedEntry();
            entry.shortName = shortNameIfNew;
            seedFile.clubs.put(clubId, entry);
            created = true;
        }

        if (!created
            && Objects.equals(entry.fullName, longName)
            && Objects.equals(entry.state, state)
            && Objects.equals(entry.email, email))
            return false;

        entry.fullName = longName;
        entry.state = state;
        entry.email = email;
        writeOrLog(configDir, seedFile);
        LOG.info("clubs.yaml: club {} meta updated (longName={}, state={}, email={})",
            clubId, longName, state, email);
        return true;
    }

    /**
     * Replaces the {@code topyacht} URL list for a club in clubs.yaml. Each value is written
     * verbatim. A null or empty {@code topyachtUrls} clears the field. If the club has no
     * entry yet, one is auto-created using {@code shortNameIfNew}. Returns true if the file
     * was changed.
     */
    static boolean updateClubTopyachtUrls(Path configDir, String clubId, String shortNameIfNew,
                                          List<String> topyachtUrls)
    {
        SeedFile seedFile = readOrNew(configDir);
        if (seedFile == null)
            return false;
        if (seedFile.clubs == null)
            seedFile.clubs = new LinkedHashMap<>();

        List<String> cleaned;
        if (topyachtUrls == null || topyachtUrls.isEmpty())
            cleaned = null;
        else
        {
            List<String> tmp = new ArrayList<>();
            for (String u : topyachtUrls)
            {
                if (u == null)
                    continue;
                String t = u.trim();
                if (!t.isEmpty() && !tmp.contains(t))
                    tmp.add(t);
            }
            cleaned = tmp.isEmpty() ? null : tmp;
        }

        SeedEntry entry = seedFile.clubs.get(clubId);
        boolean created = false;
        if (entry == null)
        {
            entry = new SeedEntry();
            entry.shortName = shortNameIfNew;
            seedFile.clubs.put(clubId, entry);
            created = true;
        }

        if (!created && Objects.equals(entry.topyacht, cleaned))
            return false;

        entry.topyacht = cleaned;
        writeOrLog(configDir, seedFile);
        LOG.info("clubs.yaml: club {} topyacht URLs updated ({} entries)",
            clubId, cleaned == null ? 0 : cleaned.size());
        return true;
    }

    /**
     * Removes a boatId from clubs.yaml — from {@code noclub} and all per-club {@code boats} lists.
     */
    static void removeBoatId(Path configDir, String boatId)
    {
        SeedFile seedFile = readOrNew(configDir);
        if (seedFile == null)
            return;

        boolean changed = false;

        if (seedFile.noclub != null && seedFile.noclub.remove(boatId))
            changed = true;

        if (seedFile.clubs != null)
            for (SeedEntry entry : seedFile.clubs.values())
            {
                if (entry.boats != null && entry.boats.remove(boatId))
                    changed = true;
            }

        if (changed)
        {
            writeOrLog(configDir, seedFile);
            LOG.info("clubs.yaml: removed boatId {}", boatId);
        }
    }

    private static SeedFile readOrNew(Path configDir)
    {
        Path file = configDir.resolve(FILENAME);
        if (Files.exists(file))
        {
            try
            {
                return YAML_MAPPER.readValue(file.toFile(), SeedFile.class);
            }
            catch (Exception e)
            {
                LOG.error("Failed to read {} for update: {}", file, e.getMessage());
                return null;
            }
        }
        return new SeedFile();
    }

    private static void writeOrLog(Path configDir, SeedFile seedFile)
    {
        Path file = configDir.resolve(FILENAME);
        try
        {
            Files.createDirectories(file.getParent());
            YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), seedFile);
        }
        catch (Exception e)
        {
            LOG.error("Failed to write {}: {}", file, e.getMessage());
        }
    }

    static class SeedFile
    {
        public Map<String, SeedEntry> clubs;
        public List<ClubOverride> boatClubOverrides;
        public List<String> noclub;
    }

    static class SeedEntry
    {
        public String shortName;
        public String state;
        public String fullName;
        public Boolean excluded;
        public String email;
        public List<String> aliases;
        public List<String> topyacht;
        public List<String> boats;
    }

    static class ClubOverride
    {
        public String sailNumber;
        public String name;
        public String clubId;
    }

    // ---- Catalogue result ----

    static class ClubCatalogue
    {
        static final ClubCatalogue EMPTY = new ClubCatalogue(null);

        /**
         * "normSail|normName" → clubId (legacy sail+name key)
         */
        private final Map<String, String> overridesByKey;
        /**
         * boatId → ordered list of clubIds that include this boat in their boats list.
         */
        private final Map<String, List<String>> boatIdToClubIds;
        /**
         * boatIds explicitly set to have no club
         */
        private final Set<String> noclubBoatIds;

        private ClubCatalogue(SeedFile file)
        {
            if (file == null)
            {
                overridesByKey = Map.of();
                boatIdToClubIds = Map.of();
                noclubBoatIds = Set.of();
                return;
            }

            // Legacy sail+name overrides
            Map<String, String> byKey = new HashMap<>();
            if (file.boatClubOverrides != null)
            {
                for (ClubOverride o : file.boatClubOverrides)
                {
                    if (o.sailNumber == null || o.name == null || o.clubId == null)
                        continue;
                    String key = IdGenerator.normaliseSailNumber(o.sailNumber)
                        + "|" + IdGenerator.normaliseName(o.name);
                    byKey.put(key, o.clubId);
                }
            }
            overridesByKey = Collections.unmodifiableMap(byKey);

            // BoatId-based: per-club boats lists (a boatId may belong to several clubs)
            Map<String, List<String>> byBoatId = new LinkedHashMap<>();
            if (file.clubs != null)
            {
                for (Map.Entry<String, SeedEntry> e : file.clubs.entrySet())
                {
                    SeedEntry entry = e.getValue();
                    if (entry.boats == null)
                        continue;
                    for (String boatId : entry.boats)
                    {
                        if (boatId == null)
                            continue;
                        byBoatId.computeIfAbsent(boatId, k -> new ArrayList<>()).add(e.getKey());
                    }
                }
            }
            Map<String, List<String>> frozen = new LinkedHashMap<>();
            byBoatId.forEach((k, v) -> frozen.put(k, List.copyOf(v)));
            boatIdToClubIds = Collections.unmodifiableMap(frozen);

            // BoatId-based: noclub list
            Set<String> noclub = new HashSet<>();
            if (file.noclub != null)
                for (String boatId : file.noclub)
                {
                    if (boatId != null)
                        noclub.add(boatId);
                }
            noclubBoatIds = Collections.unmodifiableSet(noclub);

            if (!byKey.isEmpty())
                LOG.info("Loaded club catalogue: {} sail+name override(s)", byKey.size());
            if (!boatIdToClubIds.isEmpty() || !noclub.isEmpty())
                LOG.info("Loaded club catalogue: {} boatId club assignment(s), {} no-club boatId(s)",
                    boatIdToClubIds.size(), noclub.size());
        }

        /**
         * Returns the boatId-based club override:
         * null          → no boatId-based override (fall through to sail+name lookup)
         * empty list    → explicit no-club
         * non-empty list → explicit list of clubIds (first entry is primary)
         */
        List<String> resolveBoatIdOverride(String boatId)
        {
            if (boatId == null)
                return null;
            if (noclubBoatIds.contains(boatId))
                return List.of();
            return boatIdToClubIds.get(boatId);
        }

        /**
         * Returns the override clubId for the given sail number and name, or null if none.
         */
        String resolveClubOverride(String sailNumber, String name)
        {
            if (overridesByKey.isEmpty() || sailNumber == null || name == null)
                return null;
            String key = IdGenerator.normaliseSailNumber(sailNumber)
                + "|" + IdGenerator.normaliseName(name);
            return overridesByKey.get(key);
        }
    }
}
