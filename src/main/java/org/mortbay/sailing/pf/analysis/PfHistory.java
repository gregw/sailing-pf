package org.mortbay.sailing.pf.analysis;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

/**
 * Each boat's PF as it stood at the start of each past season: the analysis rerun on the
 * data as at 1 July of each year (see {@link DataStore#asOf}). The same algorithm and
 * settings as the current PF; nothing here is a new score, only snapshots of the PF, so a
 * boat's PF can be seen staying constant, drifting or stepping over the years.
 * <p>
 * Only variants a boat had raced before the cutoff are kept, since with no races its PF is
 * just its reference factor. Held in {@code <dataRoot>/cache/pf-history.json} and recomputed
 * in full when asked, since imports, merges and settings all change the past.
 */
public final class PfHistory
{
    /** Each season's snapshot is the data as at this day of the year. */
    public static final Month CUTOFF_MONTH = Month.JULY;
    public static final int CUTOFF_DAY = 1;

    /**
     * A season is snapshotted only if at least this many races were sailed in the year
     * before its cutoff; earlier years hold too few races for a meaningful PF.
     */
    public static final int MIN_SEASON_RACES = 200;

    static final String FILE = "cache/pf-history.json";

    private static final JsonMapper MAPPER = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .serializationInclusion(JsonInclude.Include.NON_NULL)
        .build();

    /** A boat's PF in one variant at a cutoff, with its weight and the races behind it. */
    public record Point(double value, double weight, int races)
    {
    }

    /** A boat's PF at a cutoff, by variant; a variant it had not raced is null. */
    public record BoatSeason(Point spin, Point nonSpin, Point twoHanded)
    {
        static BoatSeason of(BoatPf pf)
        {
            Point spin = point(pf.spin(), pf.spinRaceCount());
            Point nonSpin = point(pf.nonSpin(), pf.nonSpinRaceCount());
            Point twoHanded = point(pf.twoHanded(), pf.twoHandedRaceCount());
            return spin == null && nonSpin == null && twoHanded == null ? null : new BoatSeason(spin, nonSpin, twoHanded);
        }

        private static Point point(Factor f, int races)
        {
            return f == null || races <= 0 ? null : new Point(f.value(), f.weight(), races);
        }
    }

    /** Every boat's PF as at one cutoff. */
    public record Season(LocalDate cutoff, Instant computed, Map<String, BoatSeason> boats)
    {
        /** The season from a PF run as at {@code cutoff}, keeping boats that had raced. */
        public static Season of(LocalDate cutoff, Map<String, BoatPf> boatPfs)
        {
            Map<String, BoatSeason> boats = new TreeMap<>();
            boatPfs.forEach((id, pf) ->
            {
                BoatSeason s = BoatSeason.of(pf);
                if (s != null)
                    boats.put(id, s);
            });
            return new Season(cutoff, Instant.now(), boats);
        }
    }

    /** One boat's PF at a cutoff, for showing its history. */
    public record BoatPoint(LocalDate cutoff, BoatSeason pf)
    {
    }

    private record Stored(List<Season> seasons)
    {
    }

    private final Path file;
    private final Map<LocalDate, Season> seasons = new TreeMap<>();

    private PfHistory(Path file)
    {
        this.file = file;
    }

    /** The history saved under {@code dataRoot}, or an empty one if none has been saved. */
    public static PfHistory load(Path dataRoot)
    {
        PfHistory history = new PfHistory(dataRoot == null ? null : dataRoot.resolve(FILE));
        if (history.file != null && Files.isRegularFile(history.file))
        {
            try
            {
                Stored stored = MAPPER.readValue(history.file.toFile(), Stored.class);
                if (stored.seasons() != null)
                    for (Season s : stored.seasons())
                        history.seasons.put(s.cutoff(), s);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException("Cannot read " + history.file, e);
            }
        }
        return history;
    }

    /** Writes the history to its file (atomically). */
    public synchronized void save()
    {
        if (file == null)
            return;
        try
        {
            Files.createDirectories(file.getParent());
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            MAPPER.writeValue(tmp.toFile(), new Stored(new ArrayList<>(seasons.values())));
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Cannot write " + file, e);
        }
    }

    public synchronized boolean has(LocalDate cutoff)
    {
        return seasons.containsKey(cutoff);
    }

    public synchronized void put(Season season)
    {
        seasons.put(season.cutoff(), season);
    }

    /** Drops seasons whose cutoff is not in {@code keep} (e.g. no longer enough races). */
    public synchronized void retain(List<LocalDate> keep)
    {
        seasons.keySet().retainAll(keep);
    }

    public synchronized List<LocalDate> cutoffs()
    {
        return List.copyOf(seasons.keySet());
    }

    /** A boat's PF at each cutoff where it had raced, oldest first. */
    public synchronized List<BoatPoint> forBoat(String boatId)
    {
        List<BoatPoint> points = new ArrayList<>();
        for (Season s : seasons.values())
        {
            BoatSeason b = s.boats().get(boatId);
            if (b != null)
                points.add(new BoatPoint(s.cutoff(), b));
        }
        return points;
    }

    /**
     * The cutoffs to snapshot: 1 July of each year, from the first whose preceding year held
     * at least {@link #MIN_SEASON_RACES} races, to the latest on or before {@code today}.
     */
    public static List<LocalDate> seasonCutoffs(DataStore store, LocalDate today)
    {
        Map<Integer, Integer> racesBySeason = new LinkedHashMap<>();
        for (Race race : store.races().values())
        {
            if (race.date() == null || store.isRaceExcluded(race.id()))
                continue;
            // The race counts towards the first cutoff after it.
            racesBySeason.merge(cutoffAfter(race.date()).getYear(), 1, Integer::sum);
        }
        LocalDate last = cutoffAfter(today).minusYears(1);
        List<LocalDate> cutoffs = new ArrayList<>();
        boolean started = false;
        for (int year = racesBySeason.keySet().stream().mapToInt(Integer::intValue).min().orElse(last.getYear() + 1);
             year <= last.getYear(); year++)
        {
            started |= racesBySeason.getOrDefault(year, 0) >= MIN_SEASON_RACES;
            if (started)
                cutoffs.add(LocalDate.of(year, CUTOFF_MONTH, CUTOFF_DAY));
        }
        return cutoffs;
    }

    /** The first cutoff after {@code date}: a race on a cutoff day is in the next season. */
    static LocalDate cutoffAfter(LocalDate date)
    {
        LocalDate c = LocalDate.of(date.getYear(), CUTOFF_MONTH, CUTOFF_DAY);
        return c.isAfter(date) ? c : c.plusYears(1);
    }
}
