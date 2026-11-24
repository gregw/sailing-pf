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
 *   <li>The easily re-imported races are purged, to be re-imported under v2 matching: the
 *       MYC archive and the BWPS importer's (BWPS, Sydney Hobart, Gold Coast), with the
 *       certificates those importers derived.</li>
 *   <li>Designs, clubs and races move up out of {@code imported/}; everything else (config,
 *       cache, logs, the MYC archive spreadsheets) is copied as is; {@code FORMAT} is 2.</li>
 * </ol>
 * Usage: {@code V2Migration [--no-purge] <v1-root> <v2-root>}; the v2 root must not exist or be
 * empty. {@code --no-purge} keeps every race and certificate, for checking that the analysis
 * of the converted database matches v1's.
 */
public final class V2Migration
{
    /** Race sources purged for re-import: the MYC archive and the BWPS importer's. */
    static boolean purgedRaceSource(String source)
    {
        return source != null && (source.startsWith("MYC spreadsheet")
            || source.equals("BWPS") || source.equals("RSHYR") || source.equals("GoldCoast"));
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
        boolean purge = !(args.length == 3 && args[0].equals("--no-purge"));
        if (args.length != (purge ? 2 : 3))
        {
            System.err.println("Usage: V2Migration [--no-purge] <v1-root> <v2-root>");
            System.exit(2);
        }
        Report report = convert(Path.of(args[args.length - 2]), Path.of(args[args.length - 1]), purge);
        System.out.print(report);
    }

    public static Report convert(Path v1, Path v2) throws IOException
    {
        return convert(v1, v2, true);
    }

    public static Report convert(Path v1, Path v2, boolean purge) throws IOException
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
                if (purge)
                    b = b.withCertificatesFiltered(c -> !purgedCertificate(c));
                report.certificatesPurged += (int)(before - b.certificates().size());
                boats.put(b.id(), b);
            }
        }

        // Races, without the purged sources
        List<RaceFile> races = new ArrayList<>();
        Path racesDir = v1.resolve("imported/races");
        if (Files.isDirectory(racesDir))
        {
            try (Stream<Path> files = Files.walk(racesDir))
            {
                for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList())
                {
                    Race r = JSON.readValue(f.toFile(), Race.class);
                    if (purge && purgedRaceSource(r.source()))
                        report.racesPurged++;
                    else
                        races.add(new RaceFile(racesDir.relativize(f), r));
                }
            }
        }

        applyAliases(v1.resolve("config"), boats, races, report);
        applyClubOverrides(v1.resolve("config"), boats, report);
        applyDesignConfig(v1.resolve("config"), v2.resolve("designs"), boats, races, report);

        // Write boats (named by current identity) and races
        Path boatsDir = Files.createDirectories(v2.resolve("boats"));
        for (Boat b : boats.values())
            JSON.writerWithDefaultPrettyPrinter().writeValue(boatsDir.resolve(b.currentId() + ".json").toFile(), b);
        report.boats = boats.size();
        for (RaceFile rf : races)
        {
            Path out = v2.resolve("races").resolve(rf.relative());
            Files.createDirectories(out.getParent());
            JSON.writerWithDefaultPrettyPrinter().writeValue(out.toFile(), rf.race());
        }
        report.racesKept = races.size();

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
        boats.put(key, new Boat(null, boat.designId(), boat.links(), orderByLastRace(identities, races),
            boat.lastUpdated(), null));
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
     * Removes the per-boat sections from the v2 copy of the config, now held on the boats:
     * aliases.yaml boats, design.yaml modifiedBoats and boatDesignOverrides, clubs.yaml per-club
     * boat lists, boatClubOverrides and noclub.
     */
    private static void stripBoatConfig(Path configDir) throws IOException
    {
        strip(configDir.resolve("aliases.yaml"), List.of("boats"), null);
        strip(configDir.resolve("design.yaml"), List.of("modifiedBoats", "boatDesignOverrides"), null);
        strip(configDir.resolve("clubs.yaml"), List.of("boatClubOverrides", "noclub"), "boats");
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
