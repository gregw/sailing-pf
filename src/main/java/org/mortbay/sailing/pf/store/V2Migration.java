package org.mortbay.sailing.pf.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.BoatAlias;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Identity;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.importer.IdGenerator;

/**
 * Converts a v1 database (an {@code imported/} tier, boats keyed by sail number and name, boat
 * aliases in {@code config/aliases.yaml}) into a new v2 database directory (boats with
 * identities; see the database v2 plan). Read only on the v1 side; run it as often as needed.
 * <ol>
 *   <li>Each v1 boat becomes a boat with one identity (its ID).</li>
 *   <li>{@code aliases.yaml} boat entries: an undated alias becomes an alias of the identity
 *       it is listed under; a dated alias (a rename) becomes another identity of that boat,
 *       with its dates, and the boat's results in its period (the v1 name-at-date rule) move
 *       to it. If another v1 boat already has that identity's ID, it joins the boat.</li>
 *   <li>{@code clubs.yaml} per-boat club overrides and {@code noclub} become the identity's
 *       clubs, set by hand (curated).</li>
 *   <li>{@code design.yaml}: {@code modifiedBoats} become the boat's modified flag; an undated
 *       {@code boatDesignOverrides} entry becomes the boat's design (re-keying its identities
 *       and results) and an alias saying its results of any design are this boat, as the
 *       override did. Dated overrides are reported (a design change makes a new boat).</li>
 *   <li>{@code design.yaml}'s design flags (excluded, ignored, noSpinnaker, oneDesign) and
 *       designYears go onto the design files (a flagged design not yet held is created), and
 *       design.yaml is retired.</li>
 *   <li>{@code clubs.yaml}'s club settings (long name, state, email, excluded, aliases, results
 *       feeds) and series types go into the club files (a club with no file gets one), and
 *       clubs.yaml is retired.</li>
 *   <li>{@code aliases.yaml} design aliases join the design files' aliases (a design named
 *       there but not yet held is created), and aliases.yaml is retired.</li>
 *   <li>Races that can be re-imported are purged, to be re-imported under v2 matching, by
 *       source ({@link Purge}): by default the MYC archive and the BWPS importer's (BWPS,
 *       Sydney Hobart, Gold Coast, with the certificates it derived); optionally the SailSys
 *       races whose cached results files are all good (replayable from the cache) and the
 *       TopYacht races with a results URL. Each purged race is kept under {@code purged/races/}
 *       (as converted), for {@link V2Restore} to put back the ones a re-import does not bring
 *       back.</li>
 *   <li>Designs, clubs and races move up out of {@code imported/}; everything else (config,
 *       cache, logs, the MYC archive spreadsheets) is copied as is; {@code FORMAT} is 2.</li>
 * </ol>
 * Usage: {@code V2Migration [--no-purge | --purge myc,bwps,sailsys,topyacht] <v1-root> <v2-root>};
 * the v2 root must not exist or be empty. {@code --no-purge} keeps every race and certificate,
 * for checking that the analysis of the converted database matches v1's.
 */
public final class V2Migration
{
    /** The sources whose races can be purged for re-import. */
    public enum Purge
    {
        /** The MYC archive spreadsheets (in myc-archive/). */
        MYC,
        /** The BWPS importer: BWPS minor races, Sydney Hobart, Gold Coast (from CYCA's feeds). */
        BWPS,
        /** SailSys races whose cached results files (cache/sailsys/races) are all good. */
        SAILSYS,
        /** TopYacht races with a results URL (re-fetched from the results sites). */
        TOPYACHT;

        static final Set<Purge> DEFAULT = Set.of(MYC, BWPS);
    }

    /** Whether a race is purged, given the sources to purge and the good SailSys cache files. */
    static boolean purged(Race r, Set<Purge> purge, Set<Integer> goodSailSysCache)
    {
        String source = r.source();
        if (source == null)
            return false;
        if (source.startsWith("MYC spreadsheet"))
            return purge.contains(Purge.MYC);
        if (source.equals("BWPS") || source.equals("RSHYR") || source.equals("GoldCoast"))
            return purge.contains(Purge.BWPS);
        if (source.startsWith("SailSys"))
        {
            if (!purge.contains(Purge.SAILSYS))
                return false;
            List<Integer> ids = new ArrayList<>();
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("SailSys-(\\d+)").matcher(source);
            while (m.find())
                ids.add(Integer.parseInt(m.group(1)));
            return !ids.isEmpty() && goodSailSysCache.containsAll(ids);
        }
        if (source.startsWith("TopYacht"))
            return purge.contains(Purge.TOPYACHT) && source.matches("(?s).*https?://.*");
        return false;
    }

    /** The race numbers of the cached SailSys races files holding results ("result": "success"). */
    static Set<Integer> goodSailSysCache(Path cacheDir) throws IOException
    {
        Set<Integer> good = new java.util.HashSet<>();
        if (!Files.isDirectory(cacheDir))
            return good;
        try (Stream<Path> files = Files.list(cacheDir))
        {
            for (Path f : files.toList())
            {
                String name = f.getFileName().toString();
                if (!name.matches("race-\\d+\\.json"))
                    continue;
                try
                {
                    JsonNode j = JSON.readTree(f.toFile());
                    if ("success".equals(j.path("result").asText()) && j.hasNonNull("data"))
                        good.add(Integer.parseInt(name.substring(5, name.length() - 5)));
                }
                catch (IOException e)
                {
                    // unreadable: not good
                }
            }
        }
        return good;
    }

    /** Certificates purged with those races: derived by the BWPS importer from its results. */
    static boolean purgedCertificate(Certificate c)
    {
        String n = c.certificateNumber();
        return n != null && (n.startsWith("bwps-") || n.startsWith("rshyr-") || n.startsWith("goldcoast-"));
    }

    private static final JsonMapper JSON = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();
    private static final JsonMapper YAML = JsonMapper.builder(new YAMLFactory())
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();

    /** What a conversion did, for the report. */
    public static final class Report
    {
        int boats;
        int racesKept;
        int racesPurged;
        int certificatesPurged;
        int aliasesAdded;
        int renameIdentities;
        int finishersMoved;
        int boatsJoined;
        int curatedClubs;
        int modified;
        int designOverrides;
        int designAliases;
        int designFlags;
        int clubsWritten;
        final List<String> notes = new ArrayList<>();

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("v2 migration\n")
                .append("  boats:                    ").append(boats).append('\n')
                .append("  aliases added:            ").append(aliasesAdded).append('\n')
                .append("  rename identities:        ").append(renameIdentities).append('\n')
                .append("  finishers moved to them:  ").append(finishersMoved).append('\n')
                .append("  boats joined (same ID):   ").append(boatsJoined).append('\n')
                .append("  identities, curated clubs: ").append(curatedClubs).append('\n')
                .append("  boats modified:           ").append(modified).append('\n')
                .append("  design overrides applied: ").append(designOverrides).append('\n')
                .append("  design aliases moved:     ").append(designAliases).append('\n')
                .append("  design flags moved:       ").append(designFlags).append('\n')
                .append("  clubs from clubs.yaml:    ").append(clubsWritten).append('\n')
                .append("  races kept:               ").append(racesKept).append('\n')
                .append("  races purged:             ").append(racesPurged).append('\n')
                .append("  certificates purged:      ").append(certificatesPurged).append('\n');
            if (!notes.isEmpty())
            {
                sb.append("notes (").append(notes.size()).append("):\n");
                notes.forEach(n -> sb.append("  ").append(n).append('\n'));
            }
            return sb.toString();
        }
    }

    /** A v1 alias from aliases.yaml. */
    private record V1Alias(String sailNumber, String name, String displayName, LocalDate from, LocalDate until)
    {
        boolean dated()
        {
            return from != null || until != null;
        }

        String shownName()
        {
            return displayName != null ? displayName : name;
        }
    }

    /** A race file and where it lives under races/. */
    private record RaceFile(Path relative, Race race)
    {
    }

    public static void main(String[] args) throws IOException
    {
        Set<Purge> purge = Purge.DEFAULT;
        int a = 0;
        if (args.length == 3 && args[0].equals("--no-purge"))
        {
            purge = Set.of();
            a = 1;
        }
        else if (args.length == 4 && args[0].equals("--purge"))
        {
            purge = java.util.EnumSet.noneOf(Purge.class);
            for (String p : args[1].split(","))
                purge.add(Purge.valueOf(p.trim().toUpperCase()));
            a = 2;
        }
        if (args.length != a + 2)
        {
            System.err.println("Usage: V2Migration [--no-purge | --purge myc,bwps,sailsys,topyacht] <v1-root> <v2-root>");
            System.exit(2);
        }
        Report report = convert(Path.of(args[a]), Path.of(args[a + 1]), purge);
        System.out.print(report);
    }

    public static Report convert(Path v1, Path v2) throws IOException
    {
        return convert(v1, v2, Purge.DEFAULT);
    }

    public static Report convert(Path v1, Path v2, boolean purge) throws IOException
    {
        return convert(v1, v2, purge ? Purge.DEFAULT : Set.of());
    }

    public static Report convert(Path v1, Path v2, Set<Purge> purge) throws IOException
    {
        if (!Files.isDirectory(v1.resolve("imported")))
            throw new IllegalArgumentException(v1 + " is not a v1 database (no imported/)");
        if (Files.exists(v2))
        {
            try (Stream<Path> s = Files.list(v2))
            {
                if (s.findAny().isPresent())
                    throw new IllegalArgumentException(v2 + " exists and is not empty");
            }
        }
        Report report = new Report();
        Files.createDirectories(v2);

        // Everything but imported/ is copied as is (config, cache, logs, MYC archive, ...)
        try (Stream<Path> top = Files.list(v1))
        {
            for (Path p : top.toList())
            {
                String name = p.getFileName().toString();
                if (!name.equals("imported") && !name.equals("FORMAT"))
                    copyTree(p, v2.resolve(name));
            }
        }
        copyTree(v1.resolve("imported/designs"), v2.resolve("designs"));
        copyTree(v1.resolve("imported/clubs"), v2.resolve("clubs"));

        // Boats, each with one identity, without purged certificates
        Map<String, Boat> boats = new TreeMap<>();
        try (Stream<Path> files = Files.list(v1.resolve("imported/boats")))
        {
            for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList())
            {
                Boat b = JSON.readValue(f.toFile(), Boat.class);
                long before = b.certificates().size();
                if (purge.contains(Purge.BWPS))
                    b = b.withCertificatesFiltered(c -> !purgedCertificate(c));
                report.certificatesPurged += (int)(before - b.certificates().size());
                boats.put(b.id(), b);
            }
        }

        // Races, all converted (so purged ones keep their identities), then split by purge
        List<RaceFile> races = new ArrayList<>();
        Path racesDir = v1.resolve("imported/races");
        if (Files.isDirectory(racesDir))
        {
            try (Stream<Path> files = Files.walk(racesDir))
            {
                for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList())
                    races.add(new RaceFile(racesDir.relativize(f), JSON.readValue(f.toFile(), Race.class)));
            }
        }
        Set<Integer> goodCache = purge.contains(Purge.SAILSYS)
            ? goodSailSysCache(v1.resolve("cache/sailsys/races")) : Set.of();

        applyAliases(v1.resolve("config"), boats, races, report);
        applyClubOverrides(v1.resolve("config"), boats, report);
        applyDesignConfig(v1.resolve("config"), v2.resolve("designs"), boats, races, report);
        moveDesignAliases(v1.resolve("config"), v2.resolve("designs"), report);
        moveDesignFlags(v1.resolve("config"), v2.resolve("designs"), report);
        moveClubSettings(v1.resolve("config"), v2.resolve("clubs"), report);

        // Write boats (named by current identity) and races
        Path boatsDir = Files.createDirectories(v2.resolve("boats"));
        for (Boat b : boats.values())
            JSON.writerWithDefaultPrettyPrinter().writeValue(boatsDir.resolve(b.currentId() + ".json").toFile(), b);
        report.boats = boats.size();
        for (RaceFile rf : races)
        {
            boolean purged = purged(rf.race(), purge, goodCache);
            Path out = v2.resolve(purged ? "purged/races" : "races").resolve(rf.relative());
            Files.createDirectories(out.getParent());
            JSON.writerWithDefaultPrettyPrinter().writeValue(out.toFile(), rf.race());
            if (purged)
                report.racesPurged++;
            else
                report.racesKept++;
        }

        stripBoatConfig(v2.resolve("config"));
        Files.writeString(v2.resolve(DataStore.FORMAT_FILE), DataStore.FORMAT + "\n");
        Files.createDirectories(v2.resolve("log"));
        Files.writeString(v2.resolve("log/migration-v2.txt"), report.toString());
        return report;
    }

    /** Applies the boat entries of aliases.yaml: aliases, and renames as identities. */
    private static void applyAliases(Path configDir, Map<String, Boat> boats, List<RaceFile> races, Report report)
        throws IOException
    {
        Path file = configDir.resolve("aliases.yaml");
        if (!Files.exists(file))
            return;
        JsonNode entries = YAML.readTree(file.toFile()).path("boats");
        if (!entries.isObject())
            return;
        for (Map.Entry<String, JsonNode> entry : (Iterable<Map.Entry<String, JsonNode>>) entries::fields)
        {
            String key = entry.getKey();
            int dash = key.indexOf('-');
            if (dash <= 0)
            {
                report.notes.add("alias entry " + key + ": not sail-name, skipped");
                continue;
            }
            String keySail = key.substring(0, dash);
            String keyName = key.substring(dash + 1);
            List<V1Alias> aliases = new ArrayList<>();
            for (JsonNode a : entry.getValue().path("aliases"))
                aliases.add(new V1Alias(text(a, "sailNumber"), text(a, "name"), text(a, "displayName"),
                    date(a, "from"), date(a, "until")));
            List<Boat> matched = boats.values().stream()
                .filter(b -> b.identities().size() >= 1 && b.identity().isNamed(keySail, keyName))
                .toList();
            if (matched.isEmpty())
            {
                report.notes.add("alias entry " + key + ": no boat with that sail number and name ("
                    + aliases.size() + " alias(es) not applied)");
                continue;
            }
            for (Boat boat : matched)
                applyAliasesTo(boat, aliases, boats, races, report);
        }
    }

    private static void applyAliasesTo(Boat boat, List<V1Alias> aliases, Map<String, Boat> boats,
                                       List<RaceFile> races, Report report)
    {
        String key = boat.id();
        Identity own = boat.identity();
        for (V1Alias a : aliases)
        {
            if (!a.dated() && (a.sailNumber() != null || a.name() != null))
            {
                Identity with = own.withAlias(new BoatAlias(a.sailNumber(), a.shownName()));
                if (with != own)
                    report.aliasesAdded++;
                own = with;
            }
        }
        Map<String, Identity> identities = new LinkedHashMap<>();
        identities.put(key, own);

        // Renames: an identity each, with the boat's results in its period
        List<V1Alias> renames = aliases.stream().filter(V1Alias::dated).toList();
        Map<V1Alias, String> renameIds = new HashMap<>();
        for (V1Alias a : renames)
        {
            String sail = a.sailNumber() != null ? a.sailNumber() : own.sailNumber();
            String id = renameId(sail, a.shownName(), boat.designId());
            if (id.equals(key))
                continue;
            renameIds.put(a, id);
            Boat other = boats.get(id);
            Identity identity;
            if (other != null && other != boat)
            {
                // The rename's ID is already a boat: the same boat under that name
                identity = other.identity();
                boats.remove(id);
                report.boatsJoined++;
                report.notes.add("rename " + a.shownName() + " of " + key + ": boat " + id + " joins it");
            }
            else
            {
                identity = new Identity(sail, a.shownName(), List.of(), List.of(), List.of("v1 rename"));
                if (a.displayName() == null)
                    report.notes.add("rename identity " + id + " of " + key + ": only the stored form of its name ("
                        + a.name() + "): edit its name");
            }
            identities.put(id, identity.withDates(a.from(), a.until()));
            report.renameIdentities++;
        }
        if (!renameIds.isEmpty())
        {
            for (int i = 0; i < races.size(); i++)
            {
                RaceFile rf = races.get(i);
                Race race = rf.race();
                if (race.date() == null || race.divisions() == null)
                    continue;
                V1Alias at = nameAt(renames, race.date());
                String to = at == null ? null : renameIds.get(at);
                if (to == null)
                    continue;
                boolean changed = false;
                List<Division> divisions = new ArrayList<>();
                for (Division d : race.divisions())
                {
                    List<Finisher> fs = new ArrayList<>();
                    for (Finisher f : d.finishers())
                    {
                        if (key.equals(f.identityId()))
                        {
                            f = f.withBoatId(to);
                            changed = true;
                            report.finishersMoved++;
                        }
                        fs.add(f);
                    }
                    divisions.add(new Division(d.name(), fs));
                }
                if (changed)
                    races.set(i, new RaceFile(rf.relative(), new Race(race.id(), race.clubId(), race.seriesIds(),
                        race.date(), race.number(), race.name(), divisions, race.source(), race.lastUpdated(), null)));
            }
        }
        if (!renameIds.isEmpty())
            distribute(key, identities, renames, renameIds, races);
        boats.put(key, new Boat(null, boat.designId(), boat.modified(), boat.links(), orderByLastRace(identities, races),
            boat.lastUpdated(), null));
    }

    /**
     * Shares out what v1 held for the whole boat, now that it has identities: each certificate
     * goes to the identity that raced most in its year (else the name in use that July, by the
     * v1 rename dates); the latest identity (the most recently raced) keeps the boat's v1 clubs
     * (else the clubs of its races, most frequent first), and each earlier identity gets only
     * the club it raced at most; a split-off identity's sources are its races' importers.
     * (Clubs set by hand in clubs.yaml are applied afterwards.)
     */
    private static void distribute(String key, Map<String, Identity> identities, List<V1Alias> renames,
                                   Map<V1Alias, String> renameIds, List<RaceFile> races)
    {
        Map<String, Map<String, Integer>> clubCounts = new HashMap<>();
        Map<String, Map<Integer, Integer>> yearCounts = new HashMap<>();
        Map<String, List<String>> raceSources = new HashMap<>();
        for (RaceFile rf : races)
        {
            Race r = rf.race();
            if (r.divisions() == null)
                continue;
            for (Division d : r.divisions())
                for (Finisher f : d.finishers())
                {
                    String id = f.identityId();
                    if (!identities.containsKey(id))
                        continue;
                    if (r.clubId() != null)
                        clubCounts.computeIfAbsent(id, k -> new HashMap<>()).merge(r.clubId(), 1, Integer::sum);
                    if (r.date() != null)
                        yearCounts.computeIfAbsent(id, k -> new HashMap<>()).merge(r.date().getYear(), 1, Integer::sum);
                    if (r.source() != null)
                    {
                        // The importer's name: "SailSys-7473" or "TopYacht - url" is SailSys or TopYacht
                        String src = r.source().trim().split("[^A-Za-z]")[0];
                        List<String> list = raceSources.computeIfAbsent(id, k -> new ArrayList<>());
                        if (!list.contains(src))
                            list.add(src);
                    }
                }
        }

        Identity own = identities.get(key);
        List<String> v1Clubs = own.clubIds();
        // Certificates, by the year each was for
        Map<String, List<Certificate>> certs = new HashMap<>();
        for (Certificate c : own.certificates())
        {
            String to = null;
            int best = 0;
            for (String id : identities.keySet())
            {
                int n = yearCounts.getOrDefault(id, Map.of()).getOrDefault(c.year(), 0);
                if (n > best)
                {
                    best = n;
                    to = id;
                }
            }
            if (to == null)
            {
                V1Alias at = nameAt(renames, LocalDate.of(c.year(), 7, 1));
                to = at == null ? key : renameIds.getOrDefault(at, key);
            }
            certs.computeIfAbsent(to, k -> new ArrayList<>()).add(c);
        }
        String latest = null;
        for (String id : orderByLastRace(identities, races).keySet())
            latest = id;
        String current = latest;
        identities.replaceAll((id, i) ->
        {
            Map<String, Integer> counts = clubCounts.getOrDefault(id, Map.of());
            List<String> ranked = counts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()))
                .map(Map.Entry::getKey).toList();
            List<String> clubs;
            if (id.equals(current))
                clubs = v1Clubs.isEmpty() ? ranked : v1Clubs;
            else
                clubs = ranked.isEmpty() ? List.of() : List.of(ranked.getFirst());
            Identity out = i.withClubIds(clubs).withCertificates(certs.getOrDefault(id, List.of()));
            if (!id.equals(key))
                for (String src : raceSources.getOrDefault(id, List.of()))
                    out = out.withSource(src);
            return out;
        });
    }

    /**
     * The v1 rule for the name a boat raced under on {@code date}: the rename with the
     * earliest {@code until} after the date, else the one with the latest {@code from} on or
     * before it, else none (its own name).
     */
    private static V1Alias nameAt(List<V1Alias> renames, LocalDate date)
    {
        V1Alias before = null, from = null;
        for (V1Alias a : renames)
        {
            if (a.until() != null && date.isBefore(a.until()) && (before == null || a.until().isBefore(before.until())))
                before = a;
            if (a.from() != null && !date.isBefore(a.from()) && (from == null || a.from().isAfter(from.from())))
                from = a;
        }
        return before != null ? before : from;
    }

    /** Identities ordered by their last race, oldest first (unraced first, in their order). */
    private static Map<String, Identity> orderByLastRace(Map<String, Identity> identities, List<RaceFile> races)
    {
        if (identities.size() < 2)
            return identities;
        Map<String, LocalDate> last = new HashMap<>();
        for (RaceFile rf : races)
        {
            Race r = rf.race();
            if (r.date() == null || r.divisions() == null)
                continue;
            for (Division d : r.divisions())
                for (Finisher f : d.finishers())
                    if (identities.containsKey(f.identityId()))
                        last.merge(f.identityId(), r.date(), (x, y) -> x.isAfter(y) ? x : y);
        }
        List<String> order = new ArrayList<>(identities.keySet());
        order.sort(Comparator.comparing((String id) -> last.get(id), Comparator.nullsFirst(Comparator.naturalOrder())));
        Map<String, Identity> out = new LinkedHashMap<>();
        order.forEach(id -> out.put(id, identities.get(id)));
        return out;
    }

    /**
     * Removes from the v2 copy of the config what the boats and designs now hold: aliases.yaml
     * (boat aliases on identities, design aliases in the design files), design.yaml
     * modifiedBoats and boatDesignOverrides, clubs.yaml per-club boat lists, boatClubOverrides
     * and noclub.
     */
    private static void stripBoatConfig(Path configDir) throws IOException
    {
        Files.deleteIfExists(configDir.resolve("aliases.yaml"));
        Files.deleteIfExists(configDir.resolve("design.yaml"));
        Files.deleteIfExists(configDir.resolve("clubs.yaml"));
    }

    /** Removes top-level {@code keys}, and {@code perClubKey} from each entry of {@code clubs}. */
    private static void strip(Path file, List<String> keys, String perClubKey) throws IOException
    {
        if (!Files.exists(file))
            return;
        JsonNode root = YAML.readTree(file.toFile());
        if (!(root instanceof com.fasterxml.jackson.databind.node.ObjectNode obj))
            return;
        keys.forEach(obj::remove);
        if (perClubKey != null && obj.get("clubs") instanceof com.fasterxml.jackson.databind.node.ObjectNode clubs)
            clubs.elements().forEachRemaining(c ->
            {
                if (c instanceof com.fasterxml.jackson.databind.node.ObjectNode co)
                    co.remove(perClubKey);
            });
        YAML.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), obj);
    }

    /** design.yaml's modifiedBoats and boatDesignOverrides, onto the boats they name. */
    private static void applyDesignConfig(Path configDir, Path designsDir, Map<String, Boat> boats,
                                          List<RaceFile> races, Report report) throws IOException
    {
        Path file = configDir.resolve("design.yaml");
        if (!Files.exists(file))
            return;
        JsonNode root = YAML.readTree(file.toFile());
        for (JsonNode m : root.path("modifiedBoats"))
        {
            String sail = text(m, "sailNumber");
            String name = text(m, "name");
            for (String key : boatsNamed(boats, sail, name))
            {
                boats.put(key, boats.get(key).withModified(true));
                report.modified++;
            }
        }
        for (JsonNode o : root.path("boatDesignOverrides"))
        {
            String designId = text(o, "designId");
            String canonicalName = text(o, "canonicalName");
            if (designId == null)
                continue;
            Path designFile = designsDir.resolve(designId + ".json");
            if (!Files.exists(designFile))
            {
                Files.createDirectories(designsDir);
                Files.writeString(designFile, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(Map.of(
                    "id", designId, "canonicalName", canonicalName != null ? canonicalName : designId,
                    "aliases", List.of(), "sources", List.of("DesignOverride"))));
            }
            for (JsonNode b : o.path("boats"))
            {
                String sail = text(b, "sailNumber");
                String name = text(b, "name");
                if (text(b, "from") != null || text(b, "until") != null)
                {
                    report.notes.add("dated design override " + sail + " " + name + " -> " + designId
                        + " not applied: a design change makes a new boat");
                    continue;
                }
                List<String> keys = boatsNamed(boats, sail, name);
                if (keys.isEmpty())
                    report.notes.add("design override " + sail + " " + name + " -> " + designId + ": no such boat");
                for (String key : keys)
                {
                    Boat boat = boats.get(key);
                    Map<String, Identity> ids = new LinkedHashMap<>(boat.identities());
                    ids.replaceAll((id, i) -> i.isNamed(sail, IdGenerator.normaliseName(name))
                        ? i.withAlias(new BoatAlias(i.sailNumber(), i.name(), BoatAlias.ANY_DESIGN)) : i);
                    boat = boat.withIdentities(ids);
                    boats.put(key, boat);
                    if (!designId.equals(boat.designId()))
                        redesign(key, designId, boats, races, report);
                    report.designOverrides++;
                }
            }
        }
    }

    /**
     * Writes the club files for a v1 {@code clubs.yaml} in {@code configDir} into {@code clubsDir}:
     * each club's settings and series types, keeping the series of an existing club file.
     * Also used by tests to set up clubs from a clubs.yaml fixture.
     */
    public static void clubsFromYaml(Path configDir, Path clubsDir) throws IOException
    {
        moveClubSettings(configDir, clubsDir, new Report());
    }

    /** clubs.yaml's club settings and series types into the club files. */
    private static void moveClubSettings(Path configDir, Path clubsDir, Report report) throws IOException
    {
        if (!Files.exists(configDir.resolve("clubs.yaml")))
            return;
        Map<String, org.mortbay.sailing.pf.data.Club> seed = ClubLoader.load(configDir);
        Map<String, org.mortbay.sailing.pf.data.SeriesType> types = ClubLoader.loadCatalogue(configDir).seriesTypes();
        Files.createDirectories(clubsDir);
        for (org.mortbay.sailing.pf.data.Club c : seed.values())
        {
            Path file = clubsDir.resolve(org.mortbay.sailing.pf.importer.IdGenerator.sanitizeIdForFilesystem(c.id()) + ".json");
            org.mortbay.sailing.pf.data.Club json = Files.exists(file)
                ? JSON.readValue(file.toFile(), org.mortbay.sailing.pf.data.Club.class) : null;
            Map<String, org.mortbay.sailing.pf.data.SeriesType> clubTypes = new TreeMap<>();
            types.forEach((seriesId, type) ->
            {
                if (seriesId.startsWith(c.id() + "/"))
                    clubTypes.put(seriesId, type);
            });
            org.mortbay.sailing.pf.data.Club merged = new org.mortbay.sailing.pf.data.Club(c.id(),
                json != null && json.shortName() != null ? json.shortName() : c.shortName(),
                c.longName(), c.state(), c.excluded(), c.email(), c.aliases(), c.topyachtGroups(), c.sailsysEvents(),
                json != null ? json.series() : List.of(), clubTypes, null);
            JSON.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), merged);
            report.clubsWritten++;
        }
    }

    /** design.yaml's design flags and years onto the design files. */
    private static void moveDesignFlags(Path configDir, Path designsDir, Report report) throws IOException
    {
        Path file = configDir.resolve("design.yaml");
        if (!Files.exists(file))
            return;
        JsonNode root = YAML.readTree(file.toFile());
        Map<String, Map<String, Object>> changes = new TreeMap<>();
        Map<String, String> names = new HashMap<>();
        for (String flag : List.of("excluded", "ignored", "noSpinnaker", "oneDesign"))
        {
            for (JsonNode n : root.path(flag))
            {
                String raw = n.asText();
                if (raw.isBlank())
                    continue;
                String id = IdGenerator.normaliseDesignName(raw);
                names.putIfAbsent(id, raw);
                changes.computeIfAbsent(id, k -> new LinkedHashMap<>()).put(flag, true);
            }
        }
        for (Map.Entry<String, JsonNode> e : (Iterable<Map.Entry<String, JsonNode>>) root.path("designYears")::fields)
        {
            if (!e.getValue().canConvertToInt())
                continue;
            String id = IdGenerator.normaliseDesignName(e.getKey());
            names.putIfAbsent(id, e.getKey());
            changes.computeIfAbsent(id, k -> new LinkedHashMap<>()).put("year", e.getValue().asInt());
        }
        for (Map.Entry<String, Map<String, Object>> c : changes.entrySet())
        {
            com.fasterxml.jackson.databind.node.ObjectNode design = designNode(designsDir, c.getKey(), names.get(c.getKey()), "design.yaml");
            c.getValue().forEach((k, v) ->
            {
                if (v instanceof Boolean b)
                    design.put(k, b);
                else
                    design.put(k, (Integer)v);
                report.designFlags++;
            });
            JSON.writerWithDefaultPrettyPrinter().writeValue(designsDir.resolve(c.getKey() + ".json").toFile(), design);
        }
    }

    /** A design file's JSON, or a new one named {@code name} if there is none. */
    private static com.fasterxml.jackson.databind.node.ObjectNode designNode(Path designsDir, String id, String name, String source)
        throws IOException
    {
        Path f = designsDir.resolve(id + ".json");
        if (Files.exists(f))
            return (com.fasterxml.jackson.databind.node.ObjectNode)JSON.readTree(f.toFile());
        Files.createDirectories(designsDir);
        com.fasterxml.jackson.databind.node.ObjectNode design = JSON.createObjectNode();
        design.put("id", id);
        design.put("canonicalName", name != null ? name : id);
        design.putArray("aliases");
        design.putArray("sources").add(source);
        return design;
    }

    /** aliases.yaml's design entries into the design files' aliases. */
    private static void moveDesignAliases(Path configDir, Path designsDir, Report report) throws IOException
    {
        Path file = configDir.resolve("aliases.yaml");
        if (!Files.exists(file))
            return;
        JsonNode entries = YAML.readTree(file.toFile()).path("designs");
        if (!entries.isObject())
            return;
        for (Map.Entry<String, JsonNode> e : (Iterable<Map.Entry<String, JsonNode>>) entries::fields)
        {
            String id = e.getKey();
            String canonicalName = text(e.getValue(), "canonicalName");
            Path designFile = designsDir.resolve(id + ".json");
            com.fasterxml.jackson.databind.node.ObjectNode design;
            if (Files.exists(designFile))
                design = (com.fasterxml.jackson.databind.node.ObjectNode)JSON.readTree(designFile.toFile());
            else
            {
                design = JSON.createObjectNode();
                design.put("id", id);
                design.put("canonicalName", canonicalName != null ? canonicalName : id);
                design.putArray("sources").add("aliases.yaml");
                Files.createDirectories(designsDir);
            }
            String name = design.path("canonicalName").asText(id);
            com.fasterxml.jackson.databind.node.ArrayNode aliases = design.get("aliases") instanceof
                com.fasterxml.jackson.databind.node.ArrayNode a ? a : design.putArray("aliases");
            List<String> have = new ArrayList<>();
            aliases.forEach(a -> have.add(a.asText().toLowerCase()));
            for (JsonNode alias : e.getValue().path("aliases"))
            {
                String a = alias.asText();
                if (a.isBlank() || a.equalsIgnoreCase(name) || have.contains(a.toLowerCase()))
                    continue;
                aliases.add(a);
                have.add(a.toLowerCase());
                report.designAliases++;
            }
            JSON.writerWithDefaultPrettyPrinter().writeValue(designFile.toFile(), design);
        }
    }

    /** The keys of the boats with an identity of this sail number and name. */
    private static List<String> boatsNamed(Map<String, Boat> boats, String sail, String name)
    {
        if (sail == null || name == null)
            return List.of();
        String normName = IdGenerator.normaliseName(name);
        return boats.entrySet().stream()
            .filter(e -> e.getValue().identities().values().stream().anyMatch(i -> i.isNamed(sail, normName)))
            .map(Map.Entry::getKey).toList();
    }

    /**
     * Moves the boat at {@code key} to {@code designId}: its identity IDs (which carry the
     * design) change, with their results; a boat already holding one of the new IDs joins it.
     */
    private static void redesign(String key, String designId, Map<String, Boat> boats, List<RaceFile> races, Report report)
    {
        Boat b = boats.get(key);
        Map<String, Identity> rekeyed = new LinkedHashMap<>();
        Map<String, String> renames = new HashMap<>();
        for (Map.Entry<String, Identity> e : b.identities().entrySet())
        {
            String newId = renameId(e.getValue().sailNumber(), e.getValue().name(), designId);
            rekeyed.put(newId, e.getValue());
            if (!newId.equals(e.getKey()))
                renames.put(e.getKey(), newId);
        }
        List<List<org.mortbay.sailing.pf.data.Link>> links = new ArrayList<>(List.of(b.links()));
        boolean modified = b.modified();
        for (String newId : renames.values())
        {
            for (Map.Entry<String, Boat> other : new ArrayList<>(boats.entrySet()))
            {
                if (other.getKey().equals(key) || !other.getValue().hasIdentity(newId))
                    continue;
                // Already recorded under the design: the same boat; its identities join
                Boat o = other.getValue();
                o.identities().forEach((id, i) -> rekeyed.merge(id, i, (mine, theirs) -> mine
                    .withCertificates(concat(mine.certificates(), theirs.certificates()))
                    .withSources(concat(mine.sources(), theirs.sources()))));
                links.add(o.links());
                modified |= o.modified();
                boats.remove(other.getKey());
                report.boatsJoined++;
                report.notes.add("design override: " + other.getKey() + " joins " + key + " (" + designId + ")");
            }
        }
        if (!renames.isEmpty())
            moveFinishers(renames, races);
        boats.put(key, new Boat(null, designId, modified, org.mortbay.sailing.pf.data.Link.merge(links),
            orderByLastRace(rekeyed, races), b.lastUpdated(), null));
    }

    private static <T> List<T> concat(List<T> a, List<T> b)
    {
        List<T> out = new ArrayList<>(a);
        b.stream().filter(x -> !out.contains(x)).forEach(out::add);
        return out;
    }

    /** Rewrites the results entered as the keys of {@code renames} to their values. */
    private static void moveFinishers(Map<String, String> renames, List<RaceFile> races)
    {
        for (int i = 0; i < races.size(); i++)
        {
            RaceFile rf = races.get(i);
            Race race = rf.race();
            if (race.divisions() == null)
                continue;
            boolean changed = false;
            List<Division> divisions = new ArrayList<>();
            for (Division d : race.divisions())
            {
                List<Finisher> fs = new ArrayList<>();
                for (Finisher f : d.finishers())
                {
                    String to = renames.get(f.identityId());
                    if (to != null)
                    {
                        f = f.withBoatId(to);
                        changed = true;
                    }
                    fs.add(f);
                }
                divisions.add(new Division(d.name(), fs));
            }
            if (changed)
                races.set(i, new RaceFile(rf.relative(), new Race(race.id(), race.clubId(), race.seriesIds(),
                    race.date(), race.number(), race.name(), divisions, race.source(), race.lastUpdated(), null)));
        }
    }

    /** clubs.yaml per-boat overrides (and noclub) become the identity's curated clubs. */
    private static void applyClubOverrides(Path configDir, Map<String, Boat> boats, Report report)
    {
        ClubLoader.ClubCatalogue catalogue = ClubLoader.loadCatalogue(configDir);
        for (Map.Entry<String, Boat> e : new ArrayList<>(boats.entrySet()))
        {
            Boat b = e.getValue();
            Map<String, Identity> ids = new LinkedHashMap<>(b.identities());
            boolean changed = false;
            for (Map.Entry<String, Identity> ie : ids.entrySet())
            {
                List<String> override = catalogue.resolveBoatIdOverride(ie.getKey());
                if (override != null)
                {
                    ie.setValue(ie.getValue().withCuratedClubIds(override));
                    report.curatedClubs++;
                    changed = true;
                }
            }
            if (changed)
                e.setValue(b.withIdentities(ids));
        }
    }

    /** The identity ID for a sail number and name of a boat of this design. */
    private static String renameId(String sail, String name, String designId)
    {
        String normSail = IdGenerator.normaliseSailNumber(sail);
        if (normSail.isEmpty())
            normSail = "nosail";
        String base = normSail + "-" + IdGenerator.normaliseName(name);
        return designId == null || designId.isBlank() ? base : base + "-" + designId;
    }

    private static String text(JsonNode n, String field)
    {
        JsonNode v = n.get(field);
        return v == null || v.isNull() || v.asText().isBlank() ? null : v.asText();
    }

    private static LocalDate date(JsonNode n, String field)
    {
        String t = text(n, field);
        return t == null ? null : LocalDate.parse(t);
    }

    private static void copyTree(Path from, Path to)
    {
        if (!Files.exists(from))
            return;
        try (Stream<Path> s = Files.walk(from))
        {
            for (Path p : s.toList())
            {
                Path target = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p))
                    Files.createDirectories(target);
                else
                {
                    Files.createDirectories(target.getParent());
                    Files.copy(p, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private V2Migration()
    {
    }
}
