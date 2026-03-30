package org.mortbay.sailing.hpf.analysis;

import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Mines paired certificate observations across boats to produce empirical conversion
 * tables between handicap systems (ORC vs IRC, AMS vs IRC), spinnaker variants
 * (spin vs non-spin), AMS two-handed vs normal, and year-over-year transitions.
 * <p>
 * All comparisons operate in TCF space: IRC/AMS values are used directly;
 * ORC GPH is converted via {@code 600 / GPH}.
 *
 * <h2>Usage</h2>
 * <pre>
 *   HandicapAnalyser analyser = new HandicapAnalyser(store);
 *   List&lt;ComparisonResult&gt; results = analyser.analyseAll();
 * </pre>
 * Can also be run as a standalone CLI (prints tables to stdout).
 */
public class HandicapAnalyser
{
    private static final Logger LOG = LoggerFactory.getLogger(HandicapAnalyser.class);
    private static final double DEFAULT_OUTLIER_SIGMA = 2.5;

    private final DataStore store;
    private final double outlierSigma;

    public HandicapAnalyser(DataStore store)
    {
        this(store, DEFAULT_OUTLIER_SIGMA);
    }

    public HandicapAnalyser(DataStore store, double outlierSigma)
    {
        this.store = store;
        this.outlierSigma = outlierSigma;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Runs all comparisons across all years present in the data store.
     */
    public List<ComparisonResult> analyseAll()
    {
        Map<ComparisonKey, List<DataPair>> pairMap = extractPairs();

        // Ensure all expected comparison keys exist (even with 0 pairs) so the
        // network graph shows every potential edge, not just those with data.
        for (ComparisonKey expected : generateExpectedKeys())
            pairMap.putIfAbsent(expected, List.of());

        List<ComparisonResult> results = new ArrayList<>();
        for (Map.Entry<ComparisonKey, List<DataPair>> entry : pairMap.entrySet())
        {
            ComparisonResult r = fitPairs(entry.getKey(), entry.getValue());
            results.add(r);
        }
        results.sort(Comparator.comparing(r -> r.key().toId()));
        LOG.info("analyseAll: {} comparisons ({} with data)",
            results.size(), results.stream().filter(r -> r.n() > 0).count());
        return results;
    }

    /**
     * Runs a single named comparison (all data, summed across all years where applicable,
     * but see {@link #analyse(String)} for the year-specific version).
     */
    public ComparisonResult analyse(String id)
    {
        ComparisonKey key = ComparisonKey.fromId(id);
        if (key == null)
            return null;

        Map<ComparisonKey, List<DataPair>> pairMap = extractPairs();
        List<DataPair> pairs = pairMap.getOrDefault(key, List.of());
        return fitPairs(key, pairs);
    }

    // -------------------------------------------------------------------------
    // Pair extraction
    // -------------------------------------------------------------------------

    /**
     * Iterates all boats and emits all pairs for all comparison types.
     * Returns a map keyed by ComparisonKey; each value is a list of (x, y) pairs in TCF.
     */
    Map<ComparisonKey, List<DataPair>> extractPairs()
    {
        Map<ComparisonKey, List<DataPair>> result = new LinkedHashMap<>();

        for (Boat boat : store.boats().values())
        {
            if (boat.certificates().isEmpty())
                continue;

            // Group certificates by (system, year, nonSpinnaker, twoHanded)
            Map<CertBucket, Certificate> best = bestCertPerBucket(boat);

            // Emit pairs for this boat
            emitCrossSystemPairs(boat.id(), best, result);
            emitVariantPairs(boat.id(), best, result);
            emitYearTransitionPairs(boat.id(), best, result);
        }

        return result;
    }

    /**
     * Generates ComparisonKeys for all potential edges in the network — year transitions,
     * cross-system comparisons, and variant comparisons — based on which
     * (system, year, variant) nodes have at least one certificate.
     * This ensures the network graph shows edges even when no boat has certificates
     * in both endpoint nodes.
     */
    private List<ComparisonKey> generateExpectedKeys()
    {
        // Collect all (system, year, nonSpin, twoHanded) tuples that exist
        Set<CertBucket> existingNodes = new HashSet<>();
        for (Boat boat : store.boats().values())
        {
            for (Certificate c : boat.certificates())
            {
                String sys = c.system();
                if (!"IRC".equals(sys) && !"ORC".equals(sys) && !"AMS".equals(sys))
                    continue;
                existingNodes.add(new CertBucket(sys, c.year(), c.nonSpinnaker(), c.twoHanded()));
            }
        }

        List<ComparisonKey> keys = new ArrayList<>();

        // Year transitions: for each (sys, variant), consecutive years where both exist
        for (String sys : new String[]{"IRC", "ORC", "AMS"})
        {
            for (boolean[] variant : new boolean[][]{{false, false}, {true, false}, {false, true}})
            {
                boolean ns = variant[0], th = variant[1];
                TreeSet<Integer> years = new TreeSet<>();
                for (CertBucket b : existingNodes)
                {
                    if (b.system().equals(sys) && b.nonSpinnaker() == ns && b.twoHanded() == th)
                        years.add(b.year());
                }
                Integer prev = null;
                for (int year : years)
                {
                    if (prev != null && year == prev + 1)
                        keys.add(yearTransitionKey(sys, ns, th, prev));
                    prev = year;
                }
            }
        }

        // Cross-system comparisons: for each year where both systems have certs
        TreeSet<Integer> allYears = new TreeSet<>();
        for (CertBucket b : existingNodes)
            allYears.add(b.year());

        for (int year : allYears)
        {
            boolean ircSpin = existingNodes.contains(new CertBucket("IRC", year, false, false));
            boolean orcSpin = existingNodes.contains(new CertBucket("ORC", year, false, false));
            boolean amsSpin = existingNodes.contains(new CertBucket("AMS", year, false, false));
            if (orcSpin && ircSpin) keys.add(ComparisonKey.orcVsIrc(year));
            if (amsSpin && ircSpin) keys.add(ComparisonKey.amsVsIrc(year));
            if (amsSpin && orcSpin) keys.add(ComparisonKey.amsVsOrc(year));

            boolean ircNs = existingNodes.contains(new CertBucket("IRC", year, true, false));
            boolean orcNs = existingNodes.contains(new CertBucket("ORC", year, true, false));
            boolean amsNs = existingNodes.contains(new CertBucket("AMS", year, true, false));
            if (orcNs && ircNs) keys.add(ComparisonKey.orcNsVsIrcNs(year));
            if (amsNs && orcNs) keys.add(ComparisonKey.amsNsVsOrcNs(year));

            boolean orcTh = existingNodes.contains(new CertBucket("ORC", year, false, true));
            boolean ircTh = existingNodes.contains(new CertBucket("IRC", year, false, true));
            boolean amsTh = existingNodes.contains(new CertBucket("AMS", year, false, true));
            if (orcTh && ircTh) keys.add(ComparisonKey.orcTwoHandedVsIrcTwoHanded(year));
            if (amsTh && orcTh) keys.add(ComparisonKey.amsTwoHandedVsOrcTwoHanded(year));

            // Variant comparisons (pooled "ALL" system)
            boolean anyNs = ircNs || orcNs || amsNs;
            boolean anySpin = ircSpin || orcSpin || amsSpin;
            boolean anyTh = ircTh || orcTh || amsTh;
            if (anyNs && anySpin) keys.add(ComparisonKey.allNsVsSpin(year));
            if (anyTh && anySpin) keys.add(ComparisonKey.allTwoHandedVsSpin(year));
        }

        return keys;
    }

    /**
     * For each (system, year, nonSpinnaker, twoHanded) combination, keep the certificate
     * with the latest expiryDate (then highest certificateNumber as tiebreak).
     */
    private Map<CertBucket, Certificate> bestCertPerBucket(Boat boat)
    {
        Map<CertBucket, Certificate> best = new HashMap<>();
        for (Certificate c : boat.certificates())
        {
            if (c.value() < 0.3 || c.value() > 3.0)
            {
                LOG.warn("Skipping implausible certificate value={} on boat={} cert={}", c.value(), boat.id(), c.certificateNumber());
                continue;
            }
            CertBucket bucket = new CertBucket(c.system(), c.year(), c.nonSpinnaker(), c.twoHanded());
            Certificate existing = best.get(bucket);
            if (existing == null || isBetter(c, existing))
                best.put(bucket, c);
        }
        return best;
    }

    private boolean isBetter(Certificate candidate, Certificate current)
    {
        // Prefer later expiry
        if (candidate.expiryDate() != null && current.expiryDate() != null)
            return candidate.expiryDate().isAfter(current.expiryDate());
        if (candidate.expiryDate() != null)
            return true;
        if (current.expiryDate() != null)
            return false;
        // Fall back to certificate number (lexicographic descending)
        if (candidate.certificateNumber() != null && current.certificateNumber() != null)
            return candidate.certificateNumber().compareTo(current.certificateNumber()) > 0;
        return false;
    }

    /** Cross-system comparisons: ORC vs IRC, AMS vs IRC (spinnaker, same year). */
    private void emitCrossSystemPairs(String boatId,
                                      Map<CertBucket, Certificate> best,
                                      Map<ComparisonKey, List<DataPair>> result)
    {
        // Collect all years present
        TreeSet<Integer> years = new TreeSet<>();
        for (CertBucket b : best.keySet())
            years.add(b.year());

        for (int year : years)
        {
            Certificate irc = best.get(new CertBucket("IRC", year, false, false));
            Certificate orc = best.get(new CertBucket("ORC", year, false, false));
            Certificate ams = best.get(new CertBucket("AMS", year, false, false));

            if (orc != null && irc != null)
                addPair(result, ComparisonKey.orcVsIrc(year), boatId,
                    toTcf(orc), toTcf(irc));

            Certificate orcNs = best.get(new CertBucket("ORC", year, true, false));
            Certificate ircNs = best.get(new CertBucket("IRC", year, true, false));
            if (orcNs != null && ircNs != null)
                addPair(result, ComparisonKey.orcNsVsIrcNs(year), boatId,
                    toTcf(orcNs), toTcf(ircNs));

            Certificate orcTwoH = best.get(new CertBucket("ORC", year, false, true));
            Certificate ircTwoH = best.get(new CertBucket("IRC", year, false, true));
            if (orcTwoH != null && ircTwoH != null)
                addPair(result, ComparisonKey.orcTwoHandedVsIrcTwoHanded(year), boatId,
                    toTcf(orcTwoH), toTcf(ircTwoH));

            if (ams != null && irc != null)
                addPair(result, ComparisonKey.amsVsIrc(year), boatId,
                    toTcf(ams), toTcf(irc));

            if (ams != null && orc != null)
                addPair(result, ComparisonKey.amsVsOrc(year), boatId,
                    toTcf(ams), toTcf(orc));

            Certificate amsNs = best.get(new CertBucket("AMS", year, true, false));
            if (amsNs != null && orcNs != null)
                addPair(result, ComparisonKey.amsNsVsOrcNs(year), boatId,
                    toTcf(amsNs), toTcf(orcNs));

            Certificate amsTwoH = best.get(new CertBucket("AMS", year, false, true));
            if (amsTwoH != null && orcTwoH != null)
                addPair(result, ComparisonKey.amsTwoHandedVsOrcTwoHanded(year), boatId,
                    toTcf(amsTwoH), toTcf(orcTwoH));
        }
    }

    /** Variant comparisons: spin vs non-spin, two-handed vs spin, pooled across all systems. */
    private void emitVariantPairs(String boatId,
                                  Map<CertBucket, Certificate> best,
                                  Map<ComparisonKey, List<DataPair>> result)
    {
        TreeSet<Integer> years = new TreeSet<>();
        for (CertBucket b : best.keySet())
            years.add(b.year());

        for (int year : years)
        {
            // NS vs spin — pool observations from all three systems into one comparison
            for (String sys : new String[]{"IRC", "ORC", "AMS"})
            {
                Certificate spin = best.get(new CertBucket(sys, year, false, false));
                Certificate nonSpin = best.get(new CertBucket(sys, year, true, false));
                if (spin != null && nonSpin != null)
                    addPair(result, ComparisonKey.allNsVsSpin(year), boatId,
                        toTcf(nonSpin), toTcf(spin));
            }

            // Two-handed vs spin — pool AMS and ORC into one comparison
            for (String sys : new String[]{"AMS", "ORC"})
            {
                Certificate normal = best.get(new CertBucket(sys, year, false, false));
                Certificate twoH = best.get(new CertBucket(sys, year, false, true));
                if (normal != null && twoH != null)
                    addPair(result, ComparisonKey.allTwoHandedVsSpin(year), boatId,
                        toTcf(twoH), toTcf(normal));
            }
        }
    }

    /** Year-transition pairs: same system, same variant, consecutive years. */
    private void emitYearTransitionPairs(String boatId,
                                         Map<CertBucket, Certificate> best,
                                         Map<ComparisonKey, List<DataPair>> result)
    {
        for (String sys : new String[]{"IRC", "ORC", "AMS"})
        {
            emitSystemYearTransitions(sys, false, false, boatId, best, result);  // spin
            emitSystemYearTransitions(sys, true,  false, boatId, best, result);  // non-spin
            emitSystemYearTransitions(sys, false, true,  boatId, best, result);  // two-handed
        }
    }

    private void emitSystemYearTransitions(String sys, boolean nonSpin, boolean twoHanded,
                                            String boatId,
                                            Map<CertBucket, Certificate> best,
                                            Map<ComparisonKey, List<DataPair>> result)
    {
        TreeSet<Integer> years = new TreeSet<>();
        for (CertBucket b : best.keySet())
        {
            if (b.system().equals(sys) && b.nonSpinnaker() == nonSpin && b.twoHanded() == twoHanded)
                years.add(b.year());
        }

        Integer prev = null;
        for (int year : years)
        {
            if (prev != null && year == prev + 1)
            {
                Certificate certPrev = best.get(new CertBucket(sys, prev,    nonSpin, twoHanded));
                Certificate certThis = best.get(new CertBucket(sys, year, nonSpin, twoHanded));
                if (certPrev != null && certThis != null)
                {
                    ComparisonKey key = yearTransitionKey(sys, nonSpin, twoHanded, prev);
                    addPair(result, key, boatId, toTcf(certPrev), toTcf(certThis));
                }
            }
            prev = year;
        }
    }

    private static ComparisonKey yearTransitionKey(String sys, boolean nonSpin,
                                                    boolean twoHanded, int yearFrom)
    {
        if (nonSpin) return switch (sys)
        {
            case "IRC" -> ComparisonKey.ircNsYearTransition(yearFrom);
            case "ORC" -> ComparisonKey.orcNsYearTransition(yearFrom);
            default    -> ComparisonKey.amsNsYearTransition(yearFrom);
        };
        if (twoHanded) return switch (sys)
        {
            case "IRC" -> ComparisonKey.ircTwoHandedYearTransition(yearFrom);
            case "ORC" -> ComparisonKey.orcTwoHandedYearTransition(yearFrom);
            default    -> ComparisonKey.amsTwoHandedYearTransition(yearFrom);
        };
        return switch (sys)
        {
            case "IRC" -> ComparisonKey.ircYearTransition(yearFrom);
            case "ORC" -> ComparisonKey.orcYearTransition(yearFrom);
            default    -> ComparisonKey.amsYearTransition(yearFrom);
        };
    }

    // -------------------------------------------------------------------------
    // OLS regression
    // -------------------------------------------------------------------------

    /**
     * Maximum fraction of pairs that may be trimmed as outliers.
     * If more than this fraction would be removed the trim is suppressed — the data
     * is probably just noisy rather than containing a handful of genuine strays.
     */
    private static final double MAX_TRIM_FRACTION = 0.20;

    /**
     * Fits pairs using a two-pass approach:
     * <ol>
     *   <li>First-pass OLS on all pairs.</li>
     *   <li>Remove any pair whose residual exceeds {@code outlierSigma} × SE,
     *       provided the trim removes ≤ {@link #MAX_TRIM_FRACTION} of the data
     *       and at least 3 pairs remain.</li>
     *   <li>Second-pass OLS on the trimmed set.</li>
     * </ol>
     * If no trimming occurs (no outliers, or guard conditions not met) the first-pass
     * fit is returned directly.
     */
    ComparisonResult fitPairs(ComparisonKey key, List<DataPair> pairs)
    {
        if (pairs.size() < 3)
            return new ComparisonResult(key, List.copyOf(pairs), List.of(), null);

        LinearFit first = fitOls(pairs);
        if (first == null)
            return new ComparisonResult(key, List.copyOf(pairs), List.of(), null);

        // Identify outliers: |residual| > outlierSigma × SE
        double threshold = outlierSigma * first.se();
        List<DataPair> kept = new ArrayList<>();
        List<DataPair> trimmed = new ArrayList<>();
        for (DataPair p : pairs)
        {
            if (Math.abs(p.y() - first.predict(p.x())) > threshold)
                trimmed.add(p);
            else
                kept.add(p);
        }

        if (!trimmed.isEmpty()
            && trimmed.size() <= pairs.size() * MAX_TRIM_FRACTION
            && kept.size() >= 3)
        {
            LinearFit second = fitOls(kept);
            if (second != null)
            {
                LOG.info("fitPairs {}: trimmed {}/{} outlier(s), R² {} → {}",
                    key.toId(), trimmed.size(), pairs.size(),
                    String.format("%.4f", first.r2()), String.format("%.4f", second.r2()));
                return new ComparisonResult(key, List.copyOf(kept), List.copyOf(trimmed), second);
            }
        }

        return new ComparisonResult(key, List.copyOf(pairs), List.of(), first);
    }

    /** Ordinary least-squares fit; returns {@code null} if degenerate (< 3 pairs or zero variance). */
    private LinearFit fitOls(List<DataPair> pairs)
    {
        int n = pairs.size();
        if (n < 3)
            return null;

        double sumX = 0, sumY = 0;
        for (DataPair p : pairs)
        {
            sumX += p.x();
            sumY += p.y();
        }
        double xMean = sumX / n;
        double yMean = sumY / n;

        double ssx = 0, ssxy = 0;
        for (DataPair p : pairs)
        {
            ssx  += (p.x() - xMean) * (p.x() - xMean);
            ssxy += (p.x() - xMean) * (p.y() - yMean);
        }

        if (ssx == 0)
            return null;  // all x values identical — degenerate

        double slope = ssxy / ssx;
        double intercept = yMean - slope * xMean;

        double ssRes = 0, ssTot = 0;
        for (DataPair p : pairs)
        {
            double predicted = slope * p.x() + intercept;
            ssRes += (p.y() - predicted) * (p.y() - predicted);
            ssTot += (p.y() - yMean) * (p.y() - yMean);
        }

        double r2 = ssTot > 0 ? Math.max(0, 1.0 - ssRes / ssTot) : 1.0;
        double se = Math.sqrt(ssRes / (n - 2));

        return new LinearFit(slope, intercept, r2, se, n, xMean, ssx);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the certificate value as TCF (time correction factor).
     * All systems now store TCF directly: IRC TCC, ORC 600/GPH (converted at import),
     * AMS TCF. No conversion needed here.
     */
    public static double toTcf(Certificate c)
    {
        return c.value();
    }

    private static void addPair(Map<ComparisonKey, List<DataPair>> map,
                                 ComparisonKey key, String boatId, double x, double y)
    {
        map.computeIfAbsent(key, k -> new ArrayList<>()).add(new DataPair(boatId, x, y));
    }

    // -------------------------------------------------------------------------
    // Standalone main
    // -------------------------------------------------------------------------

    public static void main(String[] args)
    {
        Path dataRoot = DataStore.resolveDataRoot(args);
        DataStore store = new DataStore(dataRoot);
        store.start();

        HandicapAnalyser analyser = new HandicapAnalyser(store);
        List<ComparisonResult> results = analyser.analyseAll();

        if (results.isEmpty())
        {
            System.out.println("No comparisons found — check that data is loaded.");
            store.stop();
            return;
        }

        for (ComparisonResult r : results)
        {
            System.out.println("=".repeat(72));
            System.out.printf("Comparison : %s%n", r.key().toId());
            System.out.printf("Pairs      : %d%n", r.n());

            LinearFit fit = r.fit();
            if (fit == null)
            {
                System.out.println("Fit        : insufficient data (< 3 pairs)");
                System.out.println();
                continue;
            }

            System.out.printf("R²         : %.4f%n", fit.r2());
            System.out.printf("Equation   : y = %.6f·x %+.6f%n", fit.slope(), fit.intercept());
            System.out.printf("Std error  : %.6f%n", fit.se());
            System.out.println();
            System.out.printf("  %-10s  %-14s  %-8s%n", "x (TCF)", "predicted y", "weight");
            System.out.println("  " + "-".repeat(36));
            for (double x = 0.85; x <= 1.151; x += 0.01)
            {
                double pred = fit.predict(x);
                double w = fit.weight(x);
                System.out.printf("  %-10.4f  %-14.4f  %.3f%n", x, pred, w);
            }
            System.out.println();
        }

        store.stop();
    }

    // -------------------------------------------------------------------------
    // Private helper record
    // -------------------------------------------------------------------------

    private record CertBucket(String system, int year, boolean nonSpinnaker, boolean twoHanded) {}
}
