package org.mortbay.sailing.pf.analysis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.EntryVariant;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.data.SeriesType;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Races imported more than once: the same physical race scored in several series (club
 * championship and pointscore, a female-helm subset of a twilight), under several clubs, or
 * from both SailSys and TopYacht. Each copy is its own {@link Race}, so without this a boat's
 * result would count once per copy.
 *
 * <p><b>Finding copies.</b> Two races on the same date are copies when they share at least
 * {@value #MIN_COMMON_BOATS} boats and at least 90% of the boats they share have identical
 * elapsed times — to the second, which two different races practically never give. The 10%
 * allows a time corrected in one copy and not the other. Copies of copies are one group, so a
 * race in three series is one group of three.
 *
 * <p><b>Counting each result once.</b> For each boat in more than one copy, its result
 * <em>counts</em> in the copy where its division has the most finishers (the best reference
 * time), and is <em>reference-only</em> in the others: it still helps set that division's
 * reference time — so a boat only in that copy is still measured against the boats it raced —
 * but it is not counted again in the boat's own PF, confidence, race count or charts.
 *
 * <p><b>Variant.</b> A boat's copies can disagree on its variant. A variant decided by the
 * race (series type or division name, {@link EntryVariant#isExplicit}) is believed over one
 * from the finisher's own flag. Two explicit variants that disagree are an error: logged, and
 * that boat's copies are left to count separately. When no copy is explicit, the copy with
 * the most boats of the same variant in the boat's division decides, with a warning. Every
 * copy of the boat then takes the decided variant.
 *
 * <p>Worked out from the races as stored on each analysis run; nothing is written back to the
 * races. The per-boat variant errors and warnings go to {@code <dataRoot>/log/duplicates.log},
 * rewritten on each run; the main log gets a one-line summary.
 */
public final class DuplicateRaces
{
    private static final Logger LOG = LoggerFactory.getLogger(DuplicateRaces.class);

    static final int MIN_COMMON_BOATS = 2;
    private static final double MIN_IDENTICAL_FRACTION = 0.9;

    /** A finisher: the boat in one division of one race. */
    public record FinisherKey(String raceId, int divisionIndex, String boatId) {}

    /** No duplicates: every result counts, with its own variant. */
    public static final DuplicateRaces NONE = new DuplicateRaces(Map.of(), Map.of(), Map.of(), 0, 0, 0);

    private final Map<FinisherKey, String> countedIn;
    private final Map<FinisherKey, EntryVariant> variants;
    private final Map<String, List<String>> sameRaceAs;
    private final int groups;
    private final int errors;
    private final int warnings;

    private DuplicateRaces(Map<FinisherKey, String> countedIn, Map<FinisherKey, EntryVariant> variants,
                           Map<String, List<String>> sameRaceAs, int groups, int errors, int warnings)
    {
        this.countedIn = countedIn;
        this.variants = variants;
        this.sameRaceAs = sameRaceAs;
        this.groups = groups;
        this.errors = errors;
        this.warnings = warnings;
    }

    /** True if this finisher's result counts in another copy of the race. */
    public boolean isReferenceOnly(String raceId, int divisionIndex, String boatId)
    {
        return countedIn.containsKey(new FinisherKey(raceId, divisionIndex, boatId));
    }

    /** The race in which this reference-only finisher's result counts; null if it counts here. */
    public String countedIn(String raceId, int divisionIndex, String boatId)
    {
        return countedIn.get(new FinisherKey(raceId, divisionIndex, boatId));
    }

    /** The other copies of this race, sorted by id; empty if it has none. */
    public List<String> sameRaceAs(String raceId)
    {
        return sameRaceAs.getOrDefault(raceId, List.of());
    }

    /**
     * The finisher's variant: {@link EntryVariant#of}, unless its copies disagreed and another
     * copy's variant was believed. The one place the optimiser and the REST API decide it.
     */
    public EntryVariant variant(DataStore store, Race race, int divisionIndex, Division div, Finisher f)
    {
        EntryVariant v = variants.get(new FinisherKey(race.id(), divisionIndex, f.boatId()));
        return v != null ? v : EntryVariant.of(store.raceSeriesType(race), div, f,
            store.isBoatNoSpinnakerDesign(f.boatId()));
    }

    public int groups()
    {
        return groups;
    }

    public int referenceOnlyCount()
    {
        return countedIn.size();
    }

    public int errors()
    {
        return errors;
    }

    public int warnings()
    {
        return warnings;
    }

    // ---- Finding ----

    /** One finisher in one copy, with the facts the decisions need. */
    private record Copy(Race race, int divisionIndex, Division div, Finisher f,
                        EntryVariant variant, boolean explicit, int divisionSize, int raceSize)
    {
        FinisherKey key()
        {
            return new FinisherKey(race.id(), divisionIndex, f.boatId());
        }
    }

    /** Finds the copies among the races that are not excluded (nor their club). */
    public static DuplicateRaces find(DataStore store)
    {
        Map<LocalDate, List<Race>> byDate = new TreeMap<>();
        Map<String, Map<String, Duration>> elapsedByRace = new HashMap<>();
        for (Race race : store.races().values())
        {
            if (race.date() == null || race.divisions() == null) continue;
            if (store.isRaceExcluded(race.id()) || store.isClubExcluded(race.clubId())) continue;
            Map<String, Duration> elapsed = elapsedByBoat(race);
            if (elapsed.size() < MIN_COMMON_BOATS) continue;
            elapsedByRace.put(race.id(), elapsed);
            byDate.computeIfAbsent(race.date(), d -> new ArrayList<>()).add(race);
        }

        // Union-find over race ids: copies of copies are one group.
        Map<String, String> parent = new HashMap<>();
        for (List<Race> sameDay : byDate.values())
        {
            for (int i = 0; i < sameDay.size(); i++)
            {
                for (int j = i + 1; j < sameDay.size(); j++)
                {
                    Race a = sameDay.get(i), b = sameDay.get(j);
                    if (areCopies(elapsedByRace.get(a.id()), elapsedByRace.get(b.id())))
                        union(parent, a.id(), b.id());
                }
            }
        }
        Map<String, List<Race>> groupsByRoot = new TreeMap<>();
        for (String raceId : parent.keySet())
            groupsByRoot.computeIfAbsent(find(parent, raceId), k -> new ArrayList<>())
                .add(store.races().get(raceId));

        Map<FinisherKey, String> countedIn = new LinkedHashMap<>();
        Map<FinisherKey, EntryVariant> variants = new LinkedHashMap<>();
        Map<String, List<String>> sameRaceAs = new HashMap<>();
        int errors = 0;
        int[] warnings = {0};
        List<String> details = new ArrayList<>();   // the variant errors and warnings, for duplicates.log
        for (List<Race> group : groupsByRoot.values())
        {
            group.sort(Comparator.comparing(Race::id));
            for (Race r : group)
                sameRaceAs.put(r.id(), group.stream().map(Race::id).filter(id -> !id.equals(r.id())).toList());

            Map<String, List<Copy>> copiesByBoat = new TreeMap<>();
            for (Race race : group)
            {
                SeriesType raceType = store.raceSeriesType(race);
                int raceSize = elapsedByRace.get(race.id()).size();
                for (int di = 0; di < race.divisions().size(); di++)
                {
                    Division div = race.divisions().get(di);
                    if (div.finishers() == null) continue;
                    int divSize = (int)div.finishers().stream().filter(f -> f.elapsedTime() != null).count();
                    for (Finisher f : div.finishers())
                    {
                        if (f.elapsedTime() == null || f.boatId() == null) continue;
                        boolean noSpin = store.isBoatNoSpinnakerDesign(f.boatId());
                        copiesByBoat.computeIfAbsent(f.boatId(), k -> new ArrayList<>()).add(new Copy(
                            race, di, div, f, EntryVariant.of(raceType, div, f, noSpin),
                            EntryVariant.isExplicit(raceType, div, f, noSpin), divSize, raceSize));
                    }
                }
            }

            for (Map.Entry<String, List<Copy>> e : copiesByBoat.entrySet())
            {
                List<Copy> copies = e.getValue();
                if (copies.stream().map(c -> c.race().id()).distinct().count() < 2) continue;

                EntryVariant decided = decideVariant(store, e.getKey(), copies, warnings, details);
                if (decided == null)
                {
                    errors++;
                    continue;
                }

                Copy counted = copies.stream().max(Comparator
                    .comparingInt(Copy::divisionSize)
                    .thenComparingInt(Copy::raceSize)
                    .thenComparing(c -> c.race().id(), Comparator.reverseOrder())).orElseThrow();
                for (Copy c : copies)
                {
                    if (c.variant() != decided)
                        variants.put(c.key(), decided);
                    if (c != counted)
                        countedIn.put(c.key(), counted.race().id());
                }
            }
        }

        DuplicateRaces result = new DuplicateRaces(Map.copyOf(countedIn), Map.copyOf(variants),
            Map.copyOf(sameRaceAs), groupsByRoot.size(), errors, warnings[0]);
        String summary = String.format("Duplicate races: %d groups covering %d races; %d results reference-only, "
                + "%d variant overrides, %d variant errors, %d variant warnings",
            result.groups, parent.size(), countedIn.size(), variants.size(), errors, warnings[0]);
        Path logFile = writeLog(store, summary, details);
        String where = logFile == null ? "" : " (details in " + logFile + ")";
        if (errors > 0)
            LOG.error("{}{}", summary, where);
        else
            LOG.info("{}{}", summary, where);
        return result;
    }

    /**
     * Rewrites {@code <dataRoot>/log/duplicates.log} with the summary and one line per variant
     * error or warning. Returns the file, or null if it could not be written (a warning is
     * logged; the analysis carries on).
     */
    private static Path writeLog(DataStore store, String summary, List<String> details)
    {
        if (store.dataRoot() == null)
            return null;
        try
        {
            Path logDir = store.dataRoot().resolve("log");
            Files.createDirectories(logDir);
            Path file = logDir.resolve("duplicates.log");
            StringBuilder sb = new StringBuilder();
            sb.append(Instant.now()).append(' ').append(summary).append('\n');
            details.forEach(d -> sb.append(d).append('\n'));
            Files.writeString(file, sb.toString());
            return file;
        }
        catch (IOException e)
        {
            LOG.warn("Failed to write duplicates.log: {}", e.getMessage());
            return null;
        }
    }

    /**
     * The variant all of a boat's copies take, or null (logged as an error) when two copies
     * decide it from the race and disagree. Counts a warning when none decides it from the
     * race and their own flags disagree.
     */
    private static EntryVariant decideVariant(DataStore store, String boatId, List<Copy> copies, int[] warnings,
                                              List<String> details)
    {
        List<EntryVariant> explicit = copies.stream().filter(Copy::explicit).map(Copy::variant)
            .distinct().toList();
        if (explicit.size() > 1)
        {
            details.add("ERROR boat " + boatId + " is " + describe(copies)
                + " -- the races disagree on its variant; counting each copy separately");
            return null;
        }
        if (explicit.size() == 1)
            return explicit.getFirst();
        if (copies.stream().map(Copy::variant).distinct().count() == 1)
            return copies.getFirst().variant();

        // No copy says from the race: believe the copy whose division has the most boats of
        // the variant the boat has in it.
        Copy best = copies.stream().max(Comparator
            .comparingInt((Copy c) -> sameVariantCount(store, c))
            .thenComparingInt(Copy::divisionSize)
            .thenComparing(c -> c.race().id(), Comparator.reverseOrder())).orElseThrow();
        warnings[0]++;
        details.add("WARN boat " + boatId + " is " + describe(copies)
            + " -- none set by its series or division; taking " + best.variant().code()
            + " from " + best.race().id());
        return best.variant();
    }

    private static int sameVariantCount(DataStore store, Copy c)
    {
        SeriesType raceType = store.raceSeriesType(c.race());
        int n = 0;
        for (Finisher peer : c.div().finishers())
        {
            if (peer.elapsedTime() == null || peer.boatId() == null) continue;
            if (EntryVariant.of(raceType, c.div(), peer, store.isBoatNoSpinnakerDesign(peer.boatId())) == c.variant())
                n++;
        }
        return n;
    }

    private static String describe(List<Copy> copies)
    {
        StringBuilder sb = new StringBuilder();
        for (Copy c : copies)
        {
            if (!sb.isEmpty()) sb.append(", ");
            sb.append(c.variant().code()).append(c.explicit() ? "" : " (own flag)")
                .append(" in ").append(c.race().id()).append(" [").append(c.div().name()).append(']');
        }
        return sb.toString();
    }

    /** Each boat's elapsed time in the race; a boat in two divisions keeps its first. */
    private static Map<String, Duration> elapsedByBoat(Race race)
    {
        Map<String, Duration> elapsed = new HashMap<>();
        for (Division div : race.divisions())
        {
            if (div.finishers() == null) continue;
            for (Finisher f : div.finishers())
            {
                if (f.elapsedTime() != null && f.boatId() != null)
                    elapsed.putIfAbsent(f.boatId(), f.elapsedTime());
            }
        }
        return elapsed;
    }

    /** At least MIN_COMMON_BOATS shared boats, at least 90% of them with identical times. */
    static boolean areCopies(Map<String, Duration> a, Map<String, Duration> b)
    {
        if (a.size() > b.size())
        {
            Map<String, Duration> t = a;
            a = b;
            b = t;
        }
        int common = 0, identical = 0;
        for (Map.Entry<String, Duration> e : a.entrySet())
        {
            Duration other = b.get(e.getKey());
            if (other == null) continue;
            common++;
            if (other.equals(e.getValue()))
                identical++;
        }
        return common >= MIN_COMMON_BOATS && identical >= MIN_IDENTICAL_FRACTION * common;
    }

    private static String find(Map<String, String> parent, String id)
    {
        String p = parent.get(id);
        if (p == null || p.equals(id))
            return id;
        String root = find(parent, p);
        parent.put(id, root);
        return root;
    }

    private static void union(Map<String, String> parent, String a, String b)
    {
        parent.putIfAbsent(a, a);
        parent.putIfAbsent(b, b);
        String ra = find(parent, a), rb = find(parent, b);
        if (!ra.equals(rb))
            parent.put(ra.compareTo(rb) < 0 ? rb : ra, ra.compareTo(rb) < 0 ? ra : rb);
    }
}
