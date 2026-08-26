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
import org.mortbay.sailing.pf.data.SailSysEvent;
import org.mortbay.sailing.pf.data.TopYachtGroup;
import org.mortbay.sailing.pf.importer.IdGenerator;
import org.mortbay.sailing.pf.importer.TopYachtPrefix;
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
                    entry.topyachtGroups(), entry.sailsysEvents(), List.of(), null);
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
     * Replaces the {@code topyacht} groups for a club in clubs.yaml, always writing the map
     * form (prefix → {name, urls}). Blank prefixes and duplicate URLs within a group are
     * dropped; groups left with no URLs are discarded. A null or empty {@code groups} clears
     * the field. If the club has no entry yet, one is auto-created using
     * {@code shortNameIfNew}. Returns true if the file was changed.
     */
    static boolean updateClubTopyachtGroups(Path configDir, String clubId, String shortNameIfNew,
                                            List<TopYachtGroup> groups)
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

        List<TopYachtGroup> cleaned = cleanGroups(groups);
        if (!created && Objects.equals(entry.topyachtGroups(), cleaned))
            return false;

        entry.setTopyachtGroups(cleaned);
        writeOrLog(configDir, seedFile);
        LOG.info("clubs.yaml: club {} topyacht groups updated ({} group(s), {} url(s))",
            clubId, cleaned.size(), cleaned.stream().mapToInt(g -> g.urls().size()).sum());
        return true;
    }

    /**
     * Normalises incoming groups: slugs the prefix, drops blank names, de-duplicates URLs
     * within a group, drops groups with no prefix or no URLs, and merges groups that share
     * a prefix (first name wins).
     */
    /**
     * Replaces the {@code sailsys} events for a club in clubs.yaml. A null or empty
     * {@code events} clears the field. Returns true if the file was changed.
     */
    static boolean updateClubSailsysEvents(Path configDir, String clubId, String shortNameIfNew,
                                           List<SailSysEvent> events)
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

        List<SailSysEvent> cleaned = cleanSailsysEvents(events);
        if (!created && Objects.equals(entry.sailsysEvents(), cleaned))
            return false;

        entry.setSailsysEvents(cleaned);
        writeOrLog(configDir, seedFile);
        LOG.info("clubs.yaml: club {} sailsys events updated ({} entries)", clubId, cleaned.size());
        return true;
    }

    /**
     * Normalises incoming SailSys events: derives a missing key from the club or series ID,
     * drops entries naming neither, and keeps the first entry when two share a key.
     */
    static List<SailSysEvent> cleanSailsysEvents(List<SailSysEvent> events)
    {
        if (events == null)
            return List.of();
        Map<String, SailSysEvent> byKey = new LinkedHashMap<>();
        for (SailSysEvent e : events)
        {
            if (e == null || !e.isValid())
                continue;
            String key = TopYachtPrefix.slug(e.key());
            if (key.isEmpty())
                key = e.isClub() ? "club-" + e.clubId() : "series-" + e.seriesId();
            byKey.putIfAbsent(key, new SailSysEvent(key, e.name(), e.clubId(), e.seriesId()));
        }
        return List.copyOf(byKey.values());
    }

    static List<TopYachtGroup> cleanGroups(List<TopYachtGroup> groups)
    {
        if (groups == null)
            return List.of();
        Map<String, TopYachtGroup> byPrefix = new LinkedHashMap<>();
        for (TopYachtGroup g : groups)
        {
            if (g == null)
                continue;
            String prefix = TopYachtPrefix.slug(g.prefix());
            if (prefix.isEmpty())
                continue;
            TopYachtGroup existing = byPrefix.get(prefix);
            List<String> urls = new ArrayList<>(existing != null ? existing.urls() : List.of());
            for (String u : g.urls())
            {
                if (u == null)
                    continue;
                String t = u.trim();
                if (!t.isEmpty() && !urls.contains(t))
                    urls.add(t);
            }
            String name = existing != null && existing.name() != null ? existing.name() : g.name();
            boolean merge = (existing != null && existing.mergeDivisions()) || g.mergeDivisions();
            byPrefix.put(prefix, new TopYachtGroup(prefix, name, urls, merge));
        }
        List<TopYachtGroup> out = new ArrayList<>();
        for (TopYachtGroup g : byPrefix.values())
        {
            if (!g.urls().isEmpty())
                out.add(g);
        }
        return List.copyOf(out);
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
        /**
         * Either the legacy plain list of URLs, or a map of prefix → {name, urls}.
         * Read through {@link #topyachtGroups()}, which normalises both to groups;
         * written only in the map form by {@link #setTopyachtGroups}.
         */
        public Object topyacht;
        /** Map of key -> {name?, club?|series?}. Read via {@link #sailsysEvents()}. */
        public Object sailsys;
        public List<String> boats;

        /**
         * Normalises the {@code topyacht} field to groups. A plain list is grouped by
         * derived prefix ({@link TopYachtPrefix#group}); a map is read as-is, with the key
         * as the prefix. Unrecognised shapes yield no groups.
         */
        @com.fasterxml.jackson.annotation.JsonIgnore
        List<TopYachtGroup> topyachtGroups()
        {
            if (topyacht == null)
                return List.of();
            if (topyacht instanceof List<?> list)
            {
                List<String> urls = new ArrayList<>();
                for (Object o : list)
                {
                    if (o != null)
                        urls.add(o.toString());
                }
                return TopYachtPrefix.group(urls, shortName);
            }
            if (topyacht instanceof Map<?, ?> map)
            {
                List<TopYachtGroup> groups = new ArrayList<>();
                for (Map.Entry<?, ?> e : map.entrySet())
                {
                    if (e.getKey() == null)
                        continue;
                    String prefix = TopYachtPrefix.slug(e.getKey().toString());
                    if (prefix.isEmpty())
                        continue;
                    String name = null;
                    boolean merge = false;
                    List<String> urls = new ArrayList<>();
                    if (e.getValue() instanceof Map<?, ?> body)
                    {
                        Object rawName = body.get("name");
                        if (rawName != null && !rawName.toString().isBlank())
                            name = rawName.toString();
                        merge = Boolean.TRUE.equals(body.get("merge"))
                            || "true".equalsIgnoreCase(String.valueOf(body.get("merge")));
                        if (body.get("urls") instanceof List<?> rawUrls)
                        {
                            for (Object u : rawUrls)
                            {
                                if (u != null && !u.toString().isBlank())
                                    urls.add(u.toString().trim());
                            }
                        }
                    }
                    else if (e.getValue() instanceof List<?> rawUrls)
                    {
                        // Tolerate the shorthand "prefix: [urls…]" with no name
                        for (Object u : rawUrls)
                        {
                            if (u != null && !u.toString().isBlank())
                                urls.add(u.toString().trim());
                        }
                    }
                    groups.add(new TopYachtGroup(prefix, name, urls, merge));
                }
                return List.copyOf(groups);
            }
            LOG.warn("Unrecognised topyacht entry shape for club seed: {}", topyacht.getClass());
            return List.of();
        }

        /**
         * Replaces the {@code topyacht} field with the map form, or null when empty.
         */
        /**
         * Normalises the {@code sailsys} field to events. Only the map form is accepted:
         * key -> {name?, club?, series?}. Entries naming neither a club nor a series are
         * dropped, since there would be nothing to fetch.
         */
        @com.fasterxml.jackson.annotation.JsonIgnore
        List<SailSysEvent> sailsysEvents()
        {
            if (!(sailsys instanceof Map<?, ?> map))
            {
                if (sailsys != null)
                    LOG.warn("Unrecognised sailsys entry shape for club seed: {}", sailsys.getClass());
                return List.of();
            }
            List<SailSysEvent> events = new ArrayList<>();
            for (Map.Entry<?, ?> e : map.entrySet())
            {
                if (e.getKey() == null)
                    continue;
                String key = TopYachtPrefix.slug(e.getKey().toString());
                if (key.isEmpty() || !(e.getValue() instanceof Map<?, ?> body))
                    continue;
                Object rawName = body.get("name");
                String name = rawName == null || rawName.toString().isBlank()
                    ? null : rawName.toString();
                SailSysEvent event = new SailSysEvent(key, name,
                    asInteger(body.get("club")), asInteger(body.get("series")));
                if (event.isValid())
                    events.add(event);
                else
                    LOG.warn("SailSys event '{}' names neither a club nor a series; ignoring", key);
            }
            return List.copyOf(events);
        }

        /** Replaces the {@code sailsys} field with the map form, or null when empty. */
        void setSailsysEvents(List<SailSysEvent> events)
        {
            if (events == null || events.isEmpty())
            {
                sailsys = null;
                return;
            }
            Map<String, Object> out = new LinkedHashMap<>();
            for (SailSysEvent e : events)
            {
                if (e.key() == null || e.key().isBlank() || !e.isValid())
                    continue;
                Map<String, Object> body = new LinkedHashMap<>();
                if (e.name() != null)
                    body.put("name", e.name());
                if (e.clubId() != null)
                    body.put("club", e.clubId());
                if (e.seriesId() != null)
                    body.put("series", e.seriesId());
                out.put(e.key(), body);
            }
            sailsys = out.isEmpty() ? null : out;
        }

        /** Tolerates a YAML scalar arriving as Integer, Long or String. */
        private static Integer asInteger(Object raw)
        {
            if (raw instanceof Number n)
                return n.intValue();
            if (raw == null)
                return null;
            try
            {
                return Integer.valueOf(raw.toString().trim());
            }
            catch (NumberFormatException e)
            {
                return null;
            }
        }

        void setTopyachtGroups(List<TopYachtGroup> groups)
        {
            if (groups == null || groups.isEmpty())
            {
                topyacht = null;
                return;
            }
            Map<String, Object> out = new LinkedHashMap<>();
            for (TopYachtGroup g : groups)
            {
                if (g.prefix() == null || g.prefix().isBlank() || g.urls().isEmpty())
                    continue;
                Map<String, Object> body = new LinkedHashMap<>();
                if (g.name() != null)
                    body.put("name", g.name());
                if (g.mergeDivisions())
                    body.put("merge", true);
                body.put("urls", List.copyOf(g.urls()));
                out.put(g.prefix(), body);
            }
            topyacht = out.isEmpty() ? null : out;
        }
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
