package org.mortbay.sailing.pf.analysis;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Factor;

/**
 * Builds {@link DesignProfile}s: how well established and well measured a design is in the
 * results, as six percentile-ranked spokes over the designs with at least
 * {@value #MIN_ACTIVE_BOATS} active boats. An active boat has at least
 * {@value #MIN_BOAT_RACES} distinct races in the last 12 months (as for a boat's
 * {@link PerformanceProfile}); boats flagged modified from their design are left out.
 * <p>
 * Spoke definitions, in hexagon order (Quantity → Ubiquity → Frequency → Results →
 * Consistency → Certified, then back to Quantity). Adjacent spokes multiply in the
 * polygon area ({@link PerformanceProfileBuilder#polygonArea}), so many boats count for more
 * spread over many clubs, good placings count for more over many races, and boats that sail to
 * their rating count for more when it is measured:
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
 *   <li><b>Certified</b> — share of the active boats with a measurement certificate from the
 *       last {@value #CERT_YEARS} years: the design RF rests on measurement rather than only on
 *       race results</li>
 * </ul>
 */
public class DesignProfileBuilder
{
    /** A design needs at least this many active boats to be profiled (and ranked against). */
    public static final int MIN_ACTIVE_BOATS = 3;
    /** A boat is active with at least this many distinct races in the window. */
    private static final int MIN_BOAT_RACES = 3;
    private static final int RECENT_DAYS = 365;
    /** A certificate counts for Certified if it is from the current year or this many before. */
    private static final int CERT_YEARS = 3;

    /**
     * @param residualsByBoatId PF residuals keyed by boatId
     * @param boats             all boats, by id
     * @param pfByBoatId        each boat's PF; a boat without one is left out of Consistency
     * @param designRfs         each design's RF (for Consistency's log(PF / design RF))
     * @param modified          true for a boat flagged modified from its design
     */
    public Map<String, DesignProfile> buildAll(Map<String, List<EntryResidual>> residualsByBoatId,
                                               Map<String, Boat> boats,
                                               Map<String, BoatPf> pfByBoatId,
                                               Map<String, ReferenceFactors> designRfs,
                                               Predicate<String> modified)
    {
        LocalDate cutoff = LocalDate.now().minusDays(RECENT_DAYS);
        int currentYear = LocalDate.now().getYear();

        // Active boats per design, with each boat's races in the window
        Map<String, Map<String, Set<String>>> racesByBoatByDesign = new LinkedHashMap<>();
        for (Map.Entry<String, List<EntryResidual>> e : residualsByBoatId.entrySet())
        {
            Boat boat = boats.get(e.getKey());
            if (boat == null || boat.designId() == null || modified.test(boat.id()))
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
        // placed), [4] consistency (lower = better), [5] certified
        Map<String, double[]> raw = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Set<String>>> d : racesByBoatByDesign.entrySet())
        {
            String designId = d.getKey();
            Map<String, Set<String>> racesByBoat = d.getValue();
            int n = racesByBoat.size();

            Map<String, Integer> boatsPerClub = new HashMap<>();
            int certified = 0;
            int raceCount = 0;
            List<Double> logPfOverRf = new ArrayList<>();
            double placingSum = 0;
            int placed = 0;
            for (Map.Entry<String, Set<String>> b : racesByBoat.entrySet())
            {
                Boat boat = boats.get(b.getKey());
                if (boat.primaryClubId() != null)
                    boatsPerClub.merge(boat.primaryClubId(), 1, Integer::sum);
                if (hasRecentCertificate(boat, currentYear))
                    certified++;
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
                (double) certified / n});
        }

        double[] quantity    = PerformanceProfileBuilder.percentileRanks(raw, 0, true);
        double[] ubiquity    = PerformanceProfileBuilder.percentileRanks(raw, 1, true);
        double[] frequency   = PerformanceProfileBuilder.percentileRanks(raw, 2, true);
        double[] results     = PerformanceProfileBuilder.validRanks(raw, 3, true);
        double[] consistency = PerformanceProfileBuilder.validRanks(raw, 4, false);
        double[] certifiedR  = PerformanceProfileBuilder.percentileRanks(raw, 5, true);

        String[] ids = raw.keySet().toArray(new String[0]);
        Map<String, DesignProfile> profiles = new LinkedHashMap<>();
        for (int i = 0; i < ids.length; i++)
        {
            double score = PerformanceProfileBuilder.polygonArea(new double[]{
                quantity[i], ubiquity[i], frequency[i], results[i], consistency[i], certifiedR[i]});
            profiles.put(ids[i], new DesignProfile(quantity[i], ubiquity[i], frequency[i],
                results[i], consistency[i], certifiedR[i], score, (int) raw.get(ids[i])[0]));
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

    private static boolean hasRecentCertificate(Boat boat, int currentYear)
    {
        for (Certificate c : boat.certificates())
            if (c.year() >= currentYear - CERT_YEARS && !c.isRaceDerivedOrc())
                return true;
        return false;
    }

    private static double rms(List<Double> xs)
    {
        double ss = 0;
        for (double x : xs)
            ss += x * x;
        return Math.sqrt(ss / xs.size());
    }
}
