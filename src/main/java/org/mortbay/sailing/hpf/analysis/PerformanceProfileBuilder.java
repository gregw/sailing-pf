package org.mortbay.sailing.hpf.analysis;

import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds fleet-relative {@link PerformanceProfile}s for all boats in one pass.
 * <p>
 * All five spokes use the last 12 months of data. Each spoke value is the boat's
 * percentile rank within the active fleet (boats with ≥1 finish in the window),
 * normalised to [0, 1] — 1.0 = best in fleet, 0.0 = worst.
 * <p>
 * Spoke definitions (pentagon order: Frequency → Consistency → Diversity → NonChaotic → Stability):
 * <ul>
 *   <li><b>Frequency</b> — distinct race count in last 12m (higher = better)</li>
 *   <li><b>Consistency</b> — Σ(w·r²) for residuals in last 12m (lower raw = better rank)</li>
 *   <li><b>Diversity</b> — distinct opponents raced against in last 12m (higher = better)</li>
 *   <li><b>NonChaotic</b> — Pearson correlation of |residual| with race dispersion in last 12m
 *       (higher = inconsistency is weather-driven rather than boat/crew-driven)</li>
 *   <li><b>Stability</b> — asymmetric slope penalty on weighted linear regression of residual vs date:
 *       level slope = best; improving (negative slope) = moderate penalty;
 *       declining (positive slope) = double penalty</li>
 * </ul>
 */
public class PerformanceProfileBuilder
{
    private static final int RECENT_DAYS = 365;
    private static final int MIN_NONCHAOTIC_PAIRS = 5;

    /**
     * Computes profiles for all boats with residual data, returning a map from boatId to profile.
     * Boats with no finishes in the last {@value #RECENT_DAYS} days are excluded.
     *
     * @param residualsByBoatId        HPF residuals keyed by boatId
     * @param dispersionByRaceDivision raceId → divisionName → race dispersion (weighted IQR / T₀)
     * @param races                    all races from the DataStore (for diversity lookup)
     */
    public Map<String, PerformanceProfile> buildAll(
        Map<String, List<EntryResidual>> residualsByBoatId,
        Map<String, Map<String, Double>> dispersionByRaceDivision,
        Map<String, Race> races)
    {
        LocalDate cutoff = LocalDate.now().minusDays(RECENT_DAYS);

        // --- Step 1: raw metrics per boat ---
        // [0] frequency (race count), [1] diversity (opponent count),
        // [2] sumSqAll (consistency), [3] slopePenalty (stability), [4] nonChaotic corr (NaN if insufficient)
        Map<String, double[]> raw = new LinkedHashMap<>();

        for (Map.Entry<String, List<EntryResidual>> entry : residualsByBoatId.entrySet())
        {
            String boatId = entry.getKey();
            List<EntryResidual> recent = entry.getValue().stream()
                .filter(r -> !r.raceDate().isBefore(cutoff))
                .toList();
            // Require at least 3 distinct races in the window; excludes boats with token activity
            // and prevents them from distorting the percentile rankings for active boats.
            long freq = recent.stream().map(EntryResidual::raceId).distinct().count();
            if (freq < 3) continue;

            // Diversity: distinct opponents in those races
            Set<String> raceIds = new HashSet<>();
            for (EntryResidual r : recent) raceIds.add(r.raceId());
            Set<String> opponents = new HashSet<>();
            for (String raceId : raceIds)
            {
                Race race = races.get(raceId);
                if (race == null || race.divisions() == null) continue;
                for (Division div : race.divisions())
                {
                    if (div.finishers() == null) continue;
                    for (Finisher f : div.finishers())
                        if (!boatId.equals(f.boatId())) opponents.add(f.boatId());
                }
            }

            // Consistency: weighted sum of squared residuals
            double sumSqAll = 0;
            for (EntryResidual r : recent)
                sumSqAll += r.weight() * r.residual() * r.residual();

            // Stability: asymmetric slope penalty.
            // Declining (positive slope) is penalised 2× more than improving (negative slope).
            // Level (slope ≈ 0) → penalty = 0 → best rank.
            double slopePenalty = computeSlopePenalty(recent);

            // NonChaotic: Pearson corr(|residual|, race_dispersion)
            double nonChaotic = computeNonChaotic(recent, dispersionByRaceDivision);

            raw.put(boatId, new double[]{freq, opponents.size(), sumSqAll, slopePenalty, nonChaotic});
        }

        if (raw.isEmpty()) return Map.of();

        // --- Step 2: percentile rank each metric, build profiles ---
        // Spoke order for polygon area: Frequency, Consistency, Diversity, NonChaotic, Stability
        double[] freqScores  = percentileRanks(raw, 0, true);
        double[] divScores   = percentileRanks(raw, 1, true);
        double[] consScores  = percentileRanks(raw, 2, false);
        double[] stabScores  = percentileRanks(raw, 3, false);  // lower penalty = better
        double[] ncScores    = nonChaoticRanks(raw);

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
     * Percentile ranks for NonChaotic (index 4). Boats with NaN correlation get score 0.
     * Among boats with valid correlation, higher correlation = better rank.
     */
    private static double[] nonChaoticRanks(Map<String, double[]> raw)
    {
        String[] ids = raw.keySet().toArray(new String[0]);
        int n = ids.length;
        double[] scores = new double[n];  // default 0 for NaN

        // Collect valid-data boats
        List<int[]> valid = new ArrayList<>();  // [originalIndex, sortPosition]
        for (int i = 0; i < n; i++)
            if (!Double.isNaN(raw.get(ids[i])[4]))
                valid.add(new int[]{i});
        valid.sort((a, b) -> Double.compare(raw.get(ids[a[0]])[4], raw.get(ids[b[0]])[4]));

        int m = valid.size();
        for (int rank = 0; rank < m; rank++)
        {
            double frac = m == 1 ? 0.5 : (double) rank / (m - 1);
            scores[valid.get(rank)[0]] = frac;  // higher corr = better = higher rank
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

    // --- NonChaotic raw metric ---

    private static double computeNonChaotic(
        List<EntryResidual> recent,
        Map<String, Map<String, Double>> dispersionByRaceDivision)
    {
        if (dispersionByRaceDivision == null || dispersionByRaceDivision.isEmpty())
            return Double.NaN;

        // Collect (|residual|, dispersion, weight) triples
        double[] absR = new double[recent.size()];
        double[] disp = new double[recent.size()];
        double[] wts  = new double[recent.size()];
        int count = 0;

        for (EntryResidual r : recent)
        {
            Map<String, Double> divMap = dispersionByRaceDivision.get(r.raceId());
            if (divMap == null) continue;
            Double d = divMap.get(r.divisionName());
            if (d == null) continue;
            absR[count] = Math.abs(r.residual());
            disp[count] = d;
            wts[count]  = r.weight();
            count++;
        }

        if (count < MIN_NONCHAOTIC_PAIRS) return Double.NaN;

        // Weighted Pearson correlation
        double sw = 0, swx = 0, swy = 0;
        for (int i = 0; i < count; i++) { sw += wts[i]; swx += wts[i] * absR[i]; swy += wts[i] * disp[i]; }
        double mx = swx / sw, my = swy / sw;

        double num = 0, denX = 0, denY = 0;
        for (int i = 0; i < count; i++)
        {
            double dx = absR[i] - mx, dy = disp[i] - my;
            num  += wts[i] * dx * dy;
            denX += wts[i] * dx * dx;
            denY += wts[i] * dy * dy;
        }
        double denom = Math.sqrt(denX * denY);
        return denom > 1e-12 ? num / denom : Double.NaN;
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
