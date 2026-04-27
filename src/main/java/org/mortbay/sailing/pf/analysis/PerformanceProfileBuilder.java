package org.mortbay.sailing.pf.analysis;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;

/**
 * Builds fleet-relative {@link PerformanceProfile}s for all boats in one pass.
 * <p>
 * All five spokes use the last 12 months of data. Each spoke value is the boat's
 * percentile rank within the active fleet (boats with ≥1 finish in the window),
 * normalised to [0, 1] — 1.0 = best in fleet, 0.0 = worst.
 * <p>
 * Spoke definitions (pentagon order: Frequency → Consistency → Diversity → Chaotic → Stability):
 * <ul>
 *   <li><b>Frequency</b> — duration-weighted distinct race count in last 12m, multiplied by a
 *       small year-spread bonus (boats racing across many months score slightly higher than
 *       boats concentrated in a short season)</li>
 *   <li><b>Consistency</b> — mean of asymmetrically weighted squared residuals; negative
 *       residuals (boat faster than PF) count more than positive ones, since they suggest
 *       untapped potential and a genuinely inconsistent boat. Lower raw = better rank</li>
 *   <li><b>Diversity</b> — variant-weighted Σ(√encounters) over distinct (opponent, variant)
 *       pairs; rewards both breadth of opposition and frequency of meeting it</li>
 *   <li><b>Chaotic</b> — mean squared residual weighted inversely by fleet dispersion;
 *       large residuals on calm days are penalised more than large residuals on chaotic days,
 *       so boats with small residuals in chaos rank best, and boats with large residuals in
 *       calm conditions rank worst. Lower raw = better rank</li>
 *   <li><b>Stability</b> — asymmetric slope penalty on weighted linear regression of residual vs date:
 *       level slope = best; improving (negative slope) = moderate penalty;
 *       declining (positive slope) = double penalty</li>
 * </ul>
 */
public class PerformanceProfileBuilder
{
    private static final int RECENT_DAYS = 365;
    private static final int MIN_CHAOTIC_PAIRS = 5;

    /** A race 2× longer than the fleet median counts this many times more in the Frequency spoke. */
    private static final double FREQUENCY_DURATION_SCALE = 1.2;
    /** Derived power-law exponent: 2^α = FREQUENCY_DURATION_SCALE. */
    private static final double FREQUENCY_DURATION_ALPHA =
        Math.log(FREQUENCY_DURATION_SCALE) / Math.log(2.0);
    /**
     * Year-spread floor for the Frequency multiplier. A boat with all races in a single
     * month gets this multiplier; a boat with races in 12 distinct months gets 1.0.
     * Values in between interpolate linearly. Set close to 1.0 to keep the bonus small.
     */
    private static final double FREQUENCY_SPREAD_MIN = 0.85;

    /**
     * Relative weight of negative residuals (boat faster than PF) versus positive ones
     * (slower) in the Consistency squared-residual sum. A value > 1 makes "fast" surprises
     * count more than "slow" ones — they suggest untapped potential and a genuinely
     * inconsistent boat, whereas slow days are more often explained by gear or tactics.
     */
    private static final double CONSISTENCY_FAST_WEIGHT = 1.5;

    /**
     * Floor added to dispersion in the Chaotic 1/dispersion weighting to avoid
     * blow-ups when dispersion is near zero (very tight fleet day).
     */
    private static final double DISPERSION_EPSILON = 0.01;

    private final double nonSpinDiversityWeight;
    private final double spinDiversityWeight;
    private final double twoHandedDiversityWeight;
    /**
     * Every {@code consistencyDropInterval} distinct races, one additional most-divergent result
     * is excluded from the Consistency calculation. 0 disables drops entirely.
     * E.g. interval=11 → 11 races: 1 drop, 22: 2 drops, 33: 3 drops, …
     */
    private final int consistencyDropInterval;

    /** Uses default variant weights (nonSpin=0.8, spin=1.0, twoHanded=1.2) and drop interval 11. */
    public PerformanceProfileBuilder()
    {
        this(0.8, 1.0, 1.2, 11);
    }

    public PerformanceProfileBuilder(double nonSpinDiversityWeight, double spinDiversityWeight,
                                     double twoHandedDiversityWeight, int consistencyDropInterval)
    {
        this.nonSpinDiversityWeight   = nonSpinDiversityWeight;
        this.spinDiversityWeight      = spinDiversityWeight;
        this.twoHandedDiversityWeight = twoHandedDiversityWeight;
        this.consistencyDropInterval  = consistencyDropInterval;
    }

    /**
     * Computes profiles for all boats with residual data, returning a map from boatId to profile.
     * Boats with no finishes in the last {@value #RECENT_DAYS} days are excluded.
     *
     * @param residualsByBoatId        PF residuals keyed by boatId
     * @param dispersionByRaceDivision raceId → divisionName → race dispersion (weighted IQR / T₀)
     * @param races                    all races from the DataStore (for diversity lookup)
     */
    public Map<String, PerformanceProfile> buildAll(
        Map<String, List<EntryResidual>> residualsByBoatId,
        Map<String, Map<String, Double>> dispersionByRaceDivision,
        Map<String, Race> races)
    {
        LocalDate cutoff = LocalDate.now().minusDays(RECENT_DAYS);

        // --- Pre-compute per-race-division median elapsed time (seconds) ---
        // Used to weight the Frequency spoke by race duration (dampened power law).
        Map<String, Map<String, Double>> raceDivMedianSecs = new LinkedHashMap<>();
        for (Race race : races.values())
        {
            if (race.divisions() == null) continue;
            Map<String, Double> divMap = new LinkedHashMap<>();
            for (Division div : race.divisions())
            {
                if (div.finishers() == null || div.finishers().isEmpty()) continue;
                List<Long> nanos = div.finishers().stream()
                    .filter(f -> f.elapsedTime() != null)
                    .map(f -> f.elapsedTime().toNanos())
                    .sorted()
                    .toList();
                if (!nanos.isEmpty())
                    divMap.put(div.name(), nanos.get(nanos.size() / 2) / 1e9);
            }
            if (!divMap.isEmpty()) raceDivMedianSecs.put(race.id(), divMap);
        }

        // Fleet-wide reference duration = median of all per-race-div medians within window
        List<Double> allMedians = new ArrayList<>();
        for (List<EntryResidual> residuals : residualsByBoatId.values())
            for (EntryResidual r : residuals)
            {
                if (r.raceDate().isBefore(cutoff)) continue;
                Map<String, Double> dm = raceDivMedianSecs.get(r.raceId());
                if (dm == null) continue;
                Double d = dm.get(r.divisionName());
                if (d != null) allMedians.add(d);
            }
        Collections.sort(allMedians);
        double refDurSecs = allMedians.isEmpty() ? 3600.0 : allMedians.get(allMedians.size() / 2);

        // --- Step 1: raw metrics per boat ---
        // [0] frequency (duration- and spread-weighted race count, higher = better),
        // [1] diversity (variant-weighted Σ√encounters, higher = better),
        // [2] consistency (asymmetric mean r², lower = better),
        // [3] stability (slope penalty, lower = better),
        // [4] chaotic (1/dispersion-weighted mean r², lower = better; NaN if insufficient)
        Map<String, double[]> raw = new LinkedHashMap<>();

        for (Map.Entry<String, List<EntryResidual>> entry : residualsByBoatId.entrySet())
        {
            String boatId = entry.getKey();
            List<EntryResidual> recent = entry.getValue().stream()
                .filter(r -> !r.raceDate().isBefore(cutoff))
                .toList();
            // Require at least 3 distinct races in the window; excludes boats with token activity
            // and prevents them from distorting the percentile rankings for active boats.
            long distinctRaces = recent.stream().map(EntryResidual::raceId).distinct().count();
            if (distinctRaces < 3) continue;

            // Duration-weighted frequency: each distinct race contributes (d/d_ref)^α.
            // Races at the fleet-median duration contribute 1.0 (unchanged from plain count);
            // longer races contribute slightly more, shorter ones slightly less.
            // A small year-spread bonus is then applied: a boat racing in many distinct months
            // of the 12-month window scores slightly more than one concentrated in a few months,
            // rewarding year-round participation without overwhelming the duration-weighted count.
            Set<String> seenForFreq = new HashSet<>();
            Set<YearMonth> activeMonths = new HashSet<>();
            double freq = 0;
            for (EntryResidual r : recent)
            {
                activeMonths.add(YearMonth.from(r.raceDate()));
                if (!seenForFreq.add(r.raceId())) continue;
                Map<String, Double> dm = raceDivMedianSecs.get(r.raceId());
                double durSecs = (dm != null && dm.get(r.divisionName()) != null)
                    ? dm.get(r.divisionName()) : refDurSecs;
                freq += Math.pow(durSecs / refDurSecs, FREQUENCY_DURATION_ALPHA);
            }
            int monthsActive = Math.min(activeMonths.size(), 12);
            double spreadFactor = FREQUENCY_SPREAD_MIN
                + (1.0 - FREQUENCY_SPREAD_MIN) * monthsActive / 12.0;
            freq *= spreadFactor;

            // Diversity: variant-weighted Σ(√encounters) across distinct (opponent, variant) pairs.
            // For each pair we count how many races the boat sailed against that opponent in that
            // variant, then add varWt × √count. The √ damps repeats so distinct opposition still
            // dominates the score, but a boat that frequently races against a diverse field scores
            // higher than one that has only met the same opposition once or twice.
            // Variant weights: nonSpin < spin < twoHanded — racing harder/broader variants exposes
            // the boat to a wider community of sailors.
            Map<String, Integer> raceVariant = new LinkedHashMap<>();
            for (EntryResidual r : recent)
                raceVariant.putIfAbsent(r.raceId(),
                    r.twoHanded() ? 2 : (r.nonSpinnaker() ? 1 : 0));

            // opponentBoatId → variant → encounter count
            Map<String, Map<Integer, Integer>> pairCounts = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> rve : raceVariant.entrySet())
            {
                String raceId = rve.getKey();
                int variant   = rve.getValue();
                Race race = races.get(raceId);
                if (race == null || race.divisions() == null) continue;
                for (Division div : race.divisions())
                {
                    if (div.finishers() == null) continue;
                    for (Finisher f : div.finishers())
                    {
                        if (boatId.equals(f.boatId())) continue;
                        pairCounts.computeIfAbsent(f.boatId(), k -> new LinkedHashMap<>())
                            .merge(variant, 1, Integer::sum);
                    }
                }
            }
            double diversity = 0;
            for (Map<Integer, Integer> variantMap : pairCounts.values())
            {
                for (Map.Entry<Integer, Integer> ve : variantMap.entrySet())
                {
                    int variant  = ve.getKey();
                    double varWt = variant == 2 ? twoHandedDiversityWeight
                                 : variant == 1 ? nonSpinDiversityWeight
                                 : spinDiversityWeight;
                    diversity += varWt * Math.sqrt(ve.getValue());
                }
            }

            // Consistency: mean of asymmetrically weighted squared residuals, with optional drops.
            // IRLS weights are deliberately NOT used here: they down-weight large outlier
            // residuals, which would perversely make inconsistent boats look more consistent
            // (their big outlier races contribute very little to the sum). Using the plain
            // mean ensures every race counts equally and the result is independent of fleet size.
            // Asymmetric weighting: negative residuals (boat faster than PF) are weighted
            // CONSISTENCY_FAST_WEIGHT× more than positive ones. A "fast" surprise is more
            // diagnostic of inconsistency than a "slow" one — it suggests the boat has
            // untapped potential the PF has not yet captured, whereas slow races are more
            // often explained away by gear, tactics, or a single bad start.
            // Drops: every consistencyDropInterval distinct races, one additional most-divergent
            // entry is excluded (by absolute residual) — rewarding boats with a long track record
            // by ignoring their worst results, similar to a series scoring discard.
            int drops = (consistencyDropInterval > 0) ? (int)(distinctRaces / consistencyDropInterval) : 0;
            List<EntryResidual> forConsistency;
            if (drops <= 0 || drops >= recent.size())
            {
                forConsistency = recent;
            }
            else
            {
                // Sort ascending by |residual|; drop the last `drops` (most divergent).
                forConsistency = recent.stream()
                    .sorted(Comparator.comparingDouble(r -> Math.abs(r.residual())))
                    .toList();
                forConsistency = forConsistency.subList(0, forConsistency.size() - drops);
            }
            double sumSqAll = 0;
            for (EntryResidual r : forConsistency)
            {
                double r2 = r.residual() * r.residual();
                sumSqAll += (r.residual() < 0 ? CONSISTENCY_FAST_WEIGHT : 1.0) * r2;
            }
            sumSqAll /= forConsistency.size();

            // Stability: asymmetric slope penalty.
            // Declining (positive slope) is penalised 2× more than improving (negative slope).
            // Level (slope ≈ 0) → penalty = 0 → best rank.
            double slopePenalty = computeSlopePenalty(recent);

            // Chaotic: mean r² weighted by 1/dispersion. Lower = better.
            double chaotic = computeChaotic(recent, dispersionByRaceDivision);

            raw.put(boatId, new double[]{freq, diversity, sumSqAll, slopePenalty, chaotic});
        }

        if (raw.isEmpty()) return Map.of();

        // --- Step 2: percentile rank each metric, build profiles ---
        // Spoke order for polygon area: Frequency, Consistency, Diversity, Chaotic, Stability
        double[] freqScores  = percentileRanks(raw, 0, true);
        double[] divScores   = percentileRanks(raw, 1, true);
        double[] consScores  = percentileRanks(raw, 2, false);
        double[] stabScores  = percentileRanks(raw, 3, false);  // lower penalty = better
        double[] ncScores = chaoticRanks(raw);

        String[] ids = raw.keySet().toArray(new String[0]);
        Map<String, PerformanceProfile> profiles = new LinkedHashMap<>();
        for (int i = 0; i < ids.length; i++)
        {
            double freq  = freqScores[i];
            double cons  = consScores[i];
            double div   = divScores[i];
            double nc    = ncScores[i];
            double stab  = stabScores[i];
            double score = polygonArea(new double[]{freq, cons, div, nc, stab});
            profiles.put(ids[i], new PerformanceProfile(freq, div, cons, stab, nc, score));
        }
        return Map.copyOf(profiles);
    }

    // --- Percentile ranking helpers ---

    /**
     * Returns percentile rank scores in insertion order of {@code raw}.
     * {@code idx} is the metric index. {@code higherIsBetter}: true = higher raw value = better rank.
     */
    private static double[] percentileRanks(Map<String, double[]> raw, int idx, boolean higherIsBetter)
    {
        String[] ids = raw.keySet().toArray(new String[0]);
        int n = ids.length;

        // Build sortable (value, originalIndex) pairs
        int[] order = new int[n];
        for (int i = 0; i < n; i++) order[i] = i;
        // Sort ascending by raw value
        for (int i = 0; i < n - 1; i++)
            for (int j = i + 1; j < n; j++)
                if (raw.get(ids[order[i]])[idx] > raw.get(ids[order[j]])[idx])
                { int tmp = order[i]; order[i] = order[j]; order[j] = tmp; }

        double[] scores = new double[n];
        for (int rank = 0; rank < n; rank++)
        {
            double frac = n == 1 ? 0.5 : (double) rank / (n - 1);
            scores[order[rank]] = higherIsBetter ? frac : 1.0 - frac;
        }
        return scores;
    }

    /**
     * Percentile ranks for Chaotic (index 4). Boats with NaN penalty (insufficient
     * paired observations) get score 0. Among boats with a valid penalty, lower penalty
     * = better rank (small residuals on calm days).
     */
    private static double[] chaoticRanks(Map<String, double[]> raw)
    {
        String[] ids = raw.keySet().toArray(new String[0]);
        int n = ids.length;
        double[] scores = new double[n];  // default 0 for NaN

        // Collect valid-data boats and sort ascending by penalty (lower = better).
        List<int[]> valid = new ArrayList<>();
        for (int i = 0; i < n; i++)
            if (!Double.isNaN(raw.get(ids[i])[4]))
                valid.add(new int[]{i});
        valid.sort((a, b) -> Double.compare(raw.get(ids[a[0]])[4], raw.get(ids[b[0]])[4]));

        int m = valid.size();
        for (int rank = 0; rank < m; rank++)
        {
            double frac = m == 1 ? 0.5 : (double) rank / (m - 1);
            scores[valid.get(rank)[0]] = 1.0 - frac;  // lower penalty → higher score
        }
        return scores;
    }

    // --- Stability raw metric ---

    /**
     * Computes the asymmetric slope penalty from a weighted linear regression of log-residuals
     * against race date (in fractional years).
     * <ul>
     *   <li>Level slope (≈ 0) → penalty = 0 (best)</li>
     *   <li>Improving (negative slope, boat getting faster) → penalty = |slope| × 0.5</li>
     *   <li>Declining (positive slope, boat getting slower) → penalty = |slope| × 1.0</li>
     * </ul>
     * Returns 0 if there is insufficient temporal spread to fit a slope.
     */
    private static double computeSlopePenalty(List<EntryResidual> recent)
    {
        if (recent.size() < 3) return 0.0;

        double sumW = 0, sumWt = 0, sumWr = 0;
        for (EntryResidual r : recent)
        {
            double t = r.raceDate().toEpochDay() / 365.25;
            sumW  += r.weight();
            sumWt += r.weight() * t;
            sumWr += r.weight() * r.residual();
        }
        double meanT = sumWt / sumW;
        double meanR = sumWr / sumW;

        double num = 0, den = 0;
        for (EntryResidual r : recent)
        {
            double dt = r.raceDate().toEpochDay() / 365.25 - meanT;
            num += r.weight() * dt * (r.residual() - meanR);
            den += r.weight() * dt * dt;
        }
        if (den < 1e-9) return 0.0;  // all races on same date — treat as stable

        double slope = num / den;
        // Declining: full penalty. Improving: half penalty.
        return slope > 0 ? slope : -slope * 0.5;
    }

    // --- Chaotic raw metric ---

    /**
     * Computes the Chaotic penalty: mean squared residual weighted inversely by fleet
     * dispersion. Calm conditions (low dispersion → high weight) penalise large residuals
     * heavily; chaotic conditions (high dispersion → low weight) excuse them. Therefore:
     * <ul>
     *   <li>small residuals on chaotic days → very low contribution → best rank</li>
     *   <li>large residuals on chaotic days → moderate contribution → middle rank</li>
     *   <li>large residuals on calm days → very high contribution → worst rank</li>
     * </ul>
     * Returns {@link Double#NaN} if fewer than {@link #MIN_CHAOTIC_PAIRS} races have
     * dispersion data; such boats are ranked 0 by {@link #chaoticRanks}.
     */
    private static double computeChaotic(
        List<EntryResidual> recent,
        Map<String, Map<String, Double>> dispersionByRaceDivision)
    {
        if (dispersionByRaceDivision == null || dispersionByRaceDivision.isEmpty())
            return Double.NaN;

        double sumW = 0, sumWR2 = 0;
        int count = 0;
        for (EntryResidual r : recent)
        {
            Map<String, Double> divMap = dispersionByRaceDivision.get(r.raceId());
            if (divMap == null) continue;
            Double d = divMap.get(r.divisionName());
            if (d == null) continue;
            double w = r.weight() / (d + DISPERSION_EPSILON);
            sumW   += w;
            sumWR2 += w * r.residual() * r.residual();
            count++;
        }

        if (count < MIN_CHAOTIC_PAIRS)
            return Double.NaN;
        return sumW > 1e-12 ? sumWR2 / sumW : Double.NaN;
    }

    // --- Overall score ---

    /**
     * Normalised area of the radar polygon with {@code n} equally-spaced spokes.
     * Returns 0 if any spoke is 0 (collapsed triangle), 1 if all spokes are 1.
     */
    private static double polygonArea(double[] r)
    {
        int n = r.length;
        double sinStep = Math.sin(2 * Math.PI / n);
        double area = 0;
        for (int i = 0; i < n; i++)
            area += r[i] * r[(i + 1) % n];
        area *= 0.5 * sinStep;
        return area / ((n / 2.0) * sinStep);
    }
}
