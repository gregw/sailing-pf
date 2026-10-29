package org.mortbay.sailing.pf.analysis;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Factor;

/**
 * Builds {@link DesignProfile}s: how well established and well measured a design is in the
 * results, as six percentile-ranked spokes over the designs with at least
 * {@value #MIN_ACTIVE_BOATS} active boats. An active boat has at least
 * {@value #MIN_BOAT_RACES} distinct races in the last 24 months (as for a boat's
 * {@link PerformanceProfile}). Boats flagged modified from their design count like any other:
 * they are part of the class's presence in the results, and in Consistency a class with many
 * boats modified far from its rating is rightly less consistent (the design RF itself leaves
 * them out).
 * <p>
 * Spoke definitions, in hexagon order (Quantity → Ubiquity → Frequency → Results →
 * Consistency → Modern, then back to Quantity). Adjacent spokes multiply in the polygon area
 * ({@link PerformanceProfileBuilder#polygonArea}), so many boats count for more spread over many
 * clubs, good placings count for more over many races, and a modern design counts for more
 * with many boats:
 * <ul>
 *   <li><b>Quantity</b> — active boats</li>
 *   <li><b>Ubiquity</b> — effective number of clubs, {@code 1 / Σ share²} over the active
 *       boats' primary clubs: 10 boats at one club = 1, 10 boats evenly over five clubs = 5.
 *       Not rewarded by a single boat, as clubs ÷ boats would be</li>
 *   <li><b>Frequency</b> — mean distinct races per active boat</li>
 *   <li><b>Results</b> — mean over the active boats of each boat's mean PF-corrected placing
 *       percentile ({@link PerformanceProfileBuilder#computeResults}, as for a boat's Results
 *       spoke). Each boat's PF is fitted to its own results, so designs cluster near 0.5; a
 *       design scores well when its boats usually sail at or ahead of their PFs</li>
 *   <li><b>Consistency</b> — root mean square across the active boats of log(PF / design RF),
 *       each boat in the variant it raced most: low = the boats sail to the design's rating.
 *       Against the design RF, not each boat's own: a boat without a certificate gets its RF
 *       from its own results, so its PF ≈ RF by construction. Lower raw = better rank</li>
 *   <li><b>Modern</b> — the design's year ({@link #designYears}): newer = better, so that all
 *       else being equal a newer design scores higher. A design with no year scores 0.5</li>
 * </ul>
 */
public class DesignProfileBuilder
{
    /** A design needs at least this many active boats to be profiled (and ranked against). */
    public static final int MIN_ACTIVE_BOATS = 3;
    /** A boat is active with at least this many distinct races in the window. */
    private static final int MIN_BOAT_RACES = 3;
    /** The window: 24 months, as for a boat's profile. */
    private static final int RECENT_DAYS = 730;

    /**
     * A design's year and where it came from: {@code "manual"} (design.yaml) or
     * {@code "ORC"} (the earliest age date of its ORC-certificated boats).
     */
    public record DesignYear(int year, String source, int boats) {}

    /**
     * Each design's year: the manual year if set, otherwise the earliest ORC age year
     * ({@link Certificate#ageYear}) over the design's boats, from each boat's latest ORC
     * certificate with one, leaving out boats flagged modified: the year the design first came
     * out. A wrong early age date on one boat sets it too, so set a manual year to correct it.
     *
     * @param manual the design's manual year, or null
     */
    public static Map<String, DesignYear> designYears(Map<String, Boat> boats, Predicate<String> modified,
                                                      Function<String, Integer> manual)
    {
        Map<String, List<Integer>> ages = new HashMap<>();
        for (Boat boat : boats.values())
        {
            if (boat.designId() == null || modified.test(boat.id()))
                continue;
            Certificate latest = null;
            for (Certificate c : boat.certificates())
                if (c.ageYear() != null && (latest == null || c.year() > latest.year()))
                    latest = c;
            if (latest != null)
                ages.computeIfAbsent(boat.designId(), k -> new ArrayList<>()).add(latest.ageYear());
        }
        Map<String, DesignYear> years = new HashMap<>();
        ages.forEach((designId, list) ->
        {
            years.put(designId, new DesignYear(Collections.min(list), "ORC", list.size()));
        });
        Set<String> designIds = new HashSet<>(years.keySet());
        for (Boat boat : boats.values())
            if (boat.designId() != null)
                designIds.add(boat.designId());
        for (String designId : designIds)
        {
            Integer year = manual.apply(designId);
            if (year != null)
                years.put(designId, new DesignYear(year, "manual", 0));
        }
        return years;
    }

    /**
     * @param residualsByBoatId PF residuals keyed by boatId
     * @param boats             all boats, by id
     * @param pfByBoatId        each boat's PF; a boat without one is left out of Consistency
     * @param designRfs         each design's RF (for Consistency's log(PF / design RF))
     * @param designYears       each design's year (see {@link #designYears}), for Modern
     */
    public Map<String, DesignProfile> buildAll(Map<String, List<EntryResidual>> residualsByBoatId,
                                               Map<String, Boat> boats,
                                               Map<String, BoatPf> pfByBoatId,
                                               Map<String, ReferenceFactors> designRfs,
                                               Map<String, DesignYear> designYears)
    {
        LocalDate cutoff = LocalDate.now().minusDays(RECENT_DAYS);

        // Active boats per design, with each boat's races in the window
        Map<String, Map<String, Set<String>>> racesByBoatByDesign = new LinkedHashMap<>();
        for (Map.Entry<String, List<EntryResidual>> e : residualsByBoatId.entrySet())
        {
            Boat boat = boats.get(e.getKey());
            if (boat == null || boat.designId() == null)
                continue;
            Set<String> raceIds = new HashSet<>();
            for (EntryResidual r : e.getValue())
                if (!r.raceDate().isBefore(cutoff))
                    raceIds.add(r.raceId());
            if (raceIds.size() < MIN_BOAT_RACES)
                continue;
            racesByBoatByDesign.computeIfAbsent(boat.designId(), k -> new LinkedHashMap<>())
                .put(boat.id(), raceIds);
        }
        racesByBoatByDesign.values().removeIf(m -> m.size() < MIN_ACTIVE_BOATS);
        if (racesByBoatByDesign.isEmpty())
            return Map.of();

        // Every boat's residual in each race-division of the window, for Results' placings
        Map<String, List<Double>> residualsByRaceDiv =
            PerformanceProfileBuilder.residualsByRaceDiv(residualsByBoatId, cutoff);

        // Raw metrics: [0] quantity, [1] ubiquity, [2] frequency, [3] results (NaN if no boat
        // placed), [4] consistency (lower = better), [5] modern: the design year (NaN if unknown)
        Map<String, double[]> raw = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Set<String>>> d : racesByBoatByDesign.entrySet())
        {
            String designId = d.getKey();
            Map<String, Set<String>> racesByBoat = d.getValue();
            int n = racesByBoat.size();

            Map<String, Integer> boatsPerClub = new HashMap<>();
            int raceCount = 0;
            List<Double> logPfOverRf = new ArrayList<>();
            double placingSum = 0;
            int placed = 0;
            for (Map.Entry<String, Set<String>> b : racesByBoat.entrySet())
            {
                Boat boat = boats.get(b.getKey());
                if (boat.primaryClubId() != null)
                    boatsPerClub.merge(boat.primaryClubId(), 1, Integer::sum);
                raceCount += b.getValue().size();
                Double delta = mainVariantLogPfOverRf(pfByBoatId.get(boat.id()), designRfs.get(designId));
                if (delta != null)
                    logPfOverRf.add(delta);
                List<EntryResidual> recent = residualsByBoatId.get(boat.id()).stream()
                    .filter(r -> !r.raceDate().isBefore(cutoff))
                    .toList();
                double placing = PerformanceProfileBuilder.computeResults(recent, residualsByRaceDiv);
                if (!Double.isNaN(placing))
                {
                    placingSum += placing;
                    placed++;
                }
            }

            raw.put(designId, new double[]{
                n,
                effectiveClubs(boatsPerClub),
                (double) raceCount / n,
                placed == 0 ? Double.NaN : placingSum / placed,
                logPfOverRf.size() < 2 ? Double.NaN : rms(logPfOverRf),
                designYears.containsKey(designId) ? designYears.get(designId).year() : Double.NaN});
        }

        double[] quantity    = PerformanceProfileBuilder.percentileRanks(raw, 0, true);
        double[] ubiquity    = PerformanceProfileBuilder.percentileRanks(raw, 1, true);
        double[] frequency   = PerformanceProfileBuilder.percentileRanks(raw, 2, true);
        double[] results     = PerformanceProfileBuilder.validRanks(raw, 3, true);
        double[] consistency = PerformanceProfileBuilder.validRanks(raw, 4, false);
        double[] modern      = PerformanceProfileBuilder.validRanks(raw, 5, true);

        String[] ids = raw.keySet().toArray(new String[0]);
        Map<String, DesignProfile> profiles = new LinkedHashMap<>();
        for (int i = 0; i < ids.length; i++)
        {
            if (Double.isNaN(raw.get(ids[i])[5]))
                modern[i] = 0.5;   // no design year: neutral rather than penalised
            double score = PerformanceProfileBuilder.polygonArea(new double[]{
                quantity[i], ubiquity[i], frequency[i], results[i], consistency[i], modern[i]});
            profiles.put(ids[i], new DesignProfile(quantity[i], ubiquity[i], frequency[i],
                results[i], consistency[i], modern[i], score, (int) raw.get(ids[i])[0],
                designYears.get(ids[i])));
        }
        return Map.copyOf(profiles);
    }

    /** Inverse Simpson index of boats per club: {@code 1 / Σ share²}; 0 if no boat has a club. */
    static double effectiveClubs(Map<String, Integer> boatsPerClub)
    {
        int total = boatsPerClub.values().stream().mapToInt(Integer::intValue).sum();
        if (total == 0)
            return 0;
        double sumSq = 0;
        for (int c : boatsPerClub.values())
            sumSq += ((double) c / total) * ((double) c / total);
        return 1.0 / sumSq;
    }

    /**
     * log(PF / design RF) in the variant the boat raced most, or null if it has no PF from races
     * or the design has no RF in that variant.
     */
    private static Double mainVariantLogPfOverRf(BoatPf pf, ReferenceFactors designRf)
    {
        if (pf == null || designRf == null)
            return null;
        int most = Math.max(pf.spinRaceCount(), Math.max(pf.nonSpinRaceCount(), pf.twoHandedRaceCount()));
        if (most == 0)
            return null;
        Factor boatPf, rf;
        if (most == pf.spinRaceCount())
        {
            boatPf = pf.spin();
            rf = designRf.spin();
        }
        else if (most == pf.nonSpinRaceCount())
        {
            boatPf = pf.nonSpin();
            rf = designRf.nonSpin();
        }
        else
        {
            boatPf = pf.twoHanded();
            rf = designRf.twoHanded();
        }
        if (boatPf == null || rf == null || boatPf.value() <= 0 || rf.value() <= 0)
            return null;
        return Math.log(boatPf.value() / rf.value());
    }

    private static double rms(List<Double> xs)
    {
        double ss = 0;
        for (double x : xs)
            ss += x * x;
        return Math.sqrt(ss / xs.size());
    }
}
