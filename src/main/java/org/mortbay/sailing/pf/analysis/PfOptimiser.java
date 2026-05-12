package org.mortbay.sailing.pf.analysis;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PF optimiser: weighted alternating least squares in log space.
 * <p>
 * Produces two unknowns per iteration: PF per boat-variant, and reference time T
 * per division. A regularisation term pulls each boat's PF toward its reference factor.
 * An outer loop reweights entries using residual-based outlier detection with an
 * asymmetry principle (fast outliers penalised more than slow ones).
 * <p>
 * The race unit is the Division — one T₀ per division. All boats in a division
 * sailed the same course. Mixed spin/nonSpin within a division: each boat uses its
 * own variant's PF for correction; the shared T₀ links the variants.
 */
public class PfOptimiser
{
    private static final Logger LOG = LoggerFactory.getLogger(PfOptimiser.class);

    // TODO: ARCHITECTURAL REFACTOR (independent of the no-race weight dampening fix)
    //       1. VariantConverter stores weight as a primitive instead of encapsulating Factor.
    //          Violates "conversion graphs produce Factors" — loses Factor semantics downstream.
    //       2. Factor lacks an inverse() method. Inverse conversions are currently handled
    //          ad-hoc with manual slope/intercept math; an inverse() method would clean this up.
    //       3. Confidence propagation through the conversion graph deserves a clearer model:
    //          how does path confidence compound through multi-hop conversions, and should
    //          Factor weight represent path confidence or data reliability?

    // --- Working data structures ---

    private record DivisionKey(String raceId, int divisionIndex) {}

    private record Entry(
        int boatOrdinal,        // index into logPf arrays
        int divOrdinal,         // index into logT array
        double logElapsed,      // log(elapsedTime.toNanos())
        int variant,            // 0=spin, 1=nonSpin, 2=twoHanded
        double refWeight,       // RF weight for the relevant variant
        String boatId,          // for output assembly
        String raceId,          // for output assembly
        String divisionName,    // for output assembly
        LocalDate raceDate      // for output assembly
    ) {}

    private static final int SPIN = 0;
    private static final int NON_SPIN = 1;
    private static final int TWO_HANDED = 2;

    /**
     * Log-linear approximation of the conversion graph's cross-variant edge for one
     * ordered variant pair. Stored as a fitted line in log-PF space so each ALS inner
     * iteration is one multiply-add rather than a fresh DFS traversal.
     *
     * TODO: DESIGN ISSUE — This record duplicates weight as a primitive field instead of
     *       using a Factor object.
     *       This would:
     *        - Eliminate field duplication (Factor already has weight)
     *        - Be consistent with "conversion graphs produce Factors" principle
     *        - Allow Factor.inverse() method instead of needing inverse logic here
     */
    private record VariantConverter(double slope, double intercept, double weight) {}

    /**
     * Probes the conversion graph at two seed factors per ordered variant pair and fits
     * a log-linear converter: {@code log(predicted_v) = slope × log(source_v2) + intercept}.
     * Returns null entries for pairs the graph cannot reach (e.g. missing twoHanded path).
     */
    private static VariantConverter[][] buildVariantConverters(ConversionGraph graph, int targetYear)
    {
        ConversionNode[] node = new ConversionNode[]{
            new ConversionNode("IRC", targetYear, false, false), // SPIN
            new ConversionNode("IRC", targetYear, true, false), // NON_SPIN
            new ConversionNode("IRC", targetYear, false, true)   // TWO_HANDED
        };
        VariantConverter[][] vc = new VariantConverter[3][3];
        for (int from = 0; from < 3; from++)
        {
            for (int to = 0; to < 3; to++)
            {
                if (from == to)
                    continue;
                Factor probe1 = ReferenceNetworkBuilder.convertViaGraph(
                    new Factor(1.0, 1.0), node[from], node[to], graph);
                Factor probe2 = ReferenceNetworkBuilder.convertViaGraph(
                    new Factor(2.0, 1.0), node[from], node[to], graph);
                if (probe1 == null || probe2 == null)
                    continue;
                // Two points in log-space: x1 = log(1.0) = 0, x2 = log(2.0)
                double x1 = 0.0, x2 = Math.log(2.0);
                double y1 = Math.log(probe1.value());
                double y2 = Math.log(probe2.value());
                double slope = (y2 - y1) / (x2 - x1);
                double intercept = y1; // since x1 = 0
                // Both probes carry the same seed weight (1.0); aggregated weight is the
                // graph's path-confidence for this edge. Use probe1's weight as the canonical.
                double weight = probe1.weight();
                if (weight <= 0)
                    continue;
                // TODO: CONVERSION GRAPH — probe1 returns a Factor with both value and weight.
                //       Currently we extract weight as a primitive and lose the Factor structure.
                //       Should store the entire Factor (or derive it from slope/intercept) so that:
                //       1. VariantConverter encapsulates the Factor (not duplicate weight field)
                //       2. The Factor's weight semantics are preserved through the conversion pipeline
                //       3. downstreamCode can call factor.inverse() when needed
                vc[from][to] = new VariantConverter(slope, intercept, weight);
            }
        }
        return vc;
    }

    /**
     * Backward-compatible overload: runs without the graph-driven cross-variant term.
     * Equivalent to passing {@code null} for the graph (term disabled regardless of
     * {@link PfConfig#graphCrossVariantLambda()}).
     */
    public PfResult optimise(
        DataStore store,
        Map<String, BoatDerived> boatDerivedMap,
        PfConfig config,
        Supplier<Boolean> stopCheck)
    {
        return optimise(store, boatDerivedMap, config, stopCheck, null, 0);
    }

    public PfResult optimise(
        DataStore store,
        Map<String, BoatDerived> boatDerivedMap,
        PfConfig config,
        Supplier<Boolean> stopCheck,
        ConversionGraph graph,
        int targetYear)
    {
        // --- Setup: build working data structures ---
        Map<String, Integer> boatOrdinals = new HashMap<>();
        Map<DivisionKey, Integer> divOrdinals = new HashMap<>();
        List<Entry> entries = new ArrayList<>();

        // Track which boats have entries or RFs per variant
        Map<String, double[]> boatLogRf = new HashMap<>();  // boatId → [spin, nonSpin, twoHanded] log(RF)
        Map<String, double[]> boatRfWeight = new HashMap<>(); // boatId → [spin, nonSpin, twoHanded] RF weight

        // Enumerate all non-excluded races
        for (Race race : store.races().values())
        {
            if (store.isRaceExcluded(race.id())) continue;
            if (store.isClubExcluded(race.clubId())) continue;
            if (race.divisions() == null) continue;

            // If the race's series name contains NS keywords, all finishers are non-spin
            // regardless of the per-entry SailSys flag (which reflects certificate type, not race rules)
            boolean raceForceNonSpin = false;
            if (race.seriesIds() != null)
            {
                outer:
                for (String sid : race.seriesIds())
                {
                    var club = store.clubs().get(race.clubId());
                    if (club != null && club.series() != null)
                        for (var s : club.series())
                            if (sid.equals(s.id()) && containsNonSpinKeyword(s.name()))
                            { raceForceNonSpin = true; break outer; }
                }
            }

            for (int di = 0; di < race.divisions().size(); di++)
            {
                Division div = race.divisions().get(di);
                if (div.finishers() == null || div.finishers().size() < 2) continue;

                List<Entry> divEntries = new ArrayList<>();
                for (Finisher f : div.finishers())
                {
                    if (f.elapsedTime() == null) continue;
                    if (store.isBoatExcluded(f.boatId())) continue;

                    BoatDerived bd = boatDerivedMap.get(f.boatId());
                    if (bd != null && bd.boat().designId() != null
                            && store.isDesignExcluded(bd.boat().designId())) continue;
                    if (bd == null || bd.referenceFactors() == null) continue;

                    ReferenceFactors rf = bd.referenceFactors();
                    int variant = determineVariant(f, div, raceForceNonSpin);
                    Factor rfFactor = variantFactor(rf, variant);
                    // Allow rfFactor.weight() == 0: the Step-B formula degenerates cleanly to a
                    // pure race-derived PF (no regularisation toward RF) when rfW = 0, so these
                    // boats still contribute to division time calibration and receive an PF.
                    if (rfFactor == null) continue;

                    // Ensure boat has an ordinal
                    int boatOrd = boatOrdinals.computeIfAbsent(f.boatId(), k -> boatOrdinals.size());

                    // Store RF values per boat
                    boatLogRf.computeIfAbsent(f.boatId(), k -> new double[]{Double.NaN, Double.NaN, Double.NaN});
                    boatRfWeight.computeIfAbsent(f.boatId(), k -> new double[3]);
                    boatLogRf.get(f.boatId())[variant] = Math.log(rfFactor.value());
                    boatRfWeight.get(f.boatId())[variant] = rfFactor.weight();

                    divEntries.add(new Entry(
                        boatOrd, -1, // divOrdinal set below
                        Math.log(f.elapsedTime().toNanos()),
                        variant, rfFactor.weight(),
                        f.boatId(), race.id(), div.name(), race.date()));
                }

                // Skip divisions with <2 qualifying entries
                if (divEntries.size() < 2) continue;

                DivisionKey dk = new DivisionKey(race.id(), di);
                int divOrd = divOrdinals.computeIfAbsent(dk, k -> divOrdinals.size());

                // Fix up divOrdinal in entries
                for (Entry e : divEntries)
                    entries.add(new Entry(e.boatOrdinal(), divOrd, e.logElapsed(),
                        e.variant(), e.refWeight(), e.boatId(), e.raceId(), e.divisionName(), e.raceDate()));
            }
        }

        int nBoats = boatOrdinals.size();
        int nDivs = divOrdinals.size();

        if (nBoats == 0 || nDivs == 0 || entries.isEmpty())
        {
            LOG.warn("PF optimiser: no qualifying data (boats={}, divs={}, entries={})", nBoats, nDivs, entries.size());
            return new PfResult(Map.of(), Map.of(), Map.of(), 0, 0, config, null);
        }

        LOG.info("PF optimiser: {} boats, {} divisions, {} entries", nBoats, nDivs, entries.size());

        // Build reverse map: ordinal → boatId
        String[] ordinalToBoatId = new String[nBoats];
        for (var e : boatOrdinals.entrySet())
            ordinalToBoatId[e.getValue()] = e.getKey();

        // Build reverse map: ordinal → DivisionKey
        DivisionKey[] ordinalToDivKey = new DivisionKey[nDivs];
        for (var e : divOrdinals.entrySet())
            ordinalToDivKey[e.getValue()] = e.getKey();

        // Working arrays — 3 variants per boat
        double[][] logPf = new double[3][nBoats];   // [variant][boatOrd]
        double[] logT = new double[nDivs];
        double[] entryWeights = new double[entries.size()];
        Arrays.fill(entryWeights, 1.0);

        // Initialise logPf from RF values
        for (int b = 0; b < nBoats; b++)
        {
            String boatId = ordinalToBoatId[b];
            double[] lr = boatLogRf.get(boatId);
            for (int v = 0; v < 3; v++)
                logPf[v][b] = Double.isNaN(lr[v]) ? 0.0 : lr[v];
        }

        // Per-boat per-variant entry counts
        int[][] entryCount = new int[3][nBoats];
        for (Entry e : entries)
            entryCount[e.variant()][e.boatOrdinal()]++;

        // Build per-division entry index for fast iteration
        List<List<Integer>> divEntryIndexes = new ArrayList<>();
        for (int d = 0; d < nDivs; d++)
            divEntryIndexes.add(new ArrayList<>());
        for (int i = 0; i < entries.size(); i++)
            divEntryIndexes.get(entries.get(i).divOrdinal()).add(i);

        // Build per-boat-variant entry index
        @SuppressWarnings("unchecked")
        List<Integer>[][] boatVariantEntries = new List[3][nBoats];
        for (int v = 0; v < 3; v++)
            for (int b = 0; b < nBoats; b++)
                boatVariantEntries[v][b] = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++)
        {
            Entry e = entries.get(i);
            boatVariantEntries[e.variant()][e.boatOrdinal()].add(i);
        }

        // --- Build per-variant-pair graph-converter cache ---
        // For each ordered (v2 → v) pair we probe convertViaGraph at two seed values and fit
        // a log-linear function: predLogPf_v = slope * logPf_v2 + intercept, weighted by the
        // aggregated path confidence. Built whenever a graph + targetYear are available
        // (independent of graphCrossVariantLambda — the inner loop guards its own use of varConv
        // by that lambda, but assembleResult uses varConv unconditionally to dampen the weight
        // of variants for which the boat has no race entries).
        VariantConverter[][] varConv =
            (graph != null && targetYear > 0)
                ? buildVariantConverters(graph, targetYear)
                : null;

        // --- Step 13: Initial T₀ per division (weighted median of corrected times) ---
        double[] divIqrLog = new double[nDivs];
        computeT0(entries, divEntryIndexes, logPf, entryWeights, logT, divIqrLog);

        // --- Step 15: Initial entry weights ---
        computeEntryWeights(entries, entryWeights, logPf, logT, divIqrLog, config);

        // --- Outer reweighting loop ---
        int totalInner = 0;
        int outerIter;
        boolean innerConverged = false;
        boolean outerConverged = false;
        double finalMaxDelta = 0;
        double finalMaxWeightChange = 0;
        List<Double> outerDeltaTrace = new ArrayList<>();
        for (outerIter = 0; outerIter < config.maxOuterIterations(); outerIter++)
        {
            if (stopCheck.get())
            {
                LOG.info("PF optimiser: stopped by request after {} outer iterations", outerIter);
                return new PfResult(Map.of(), Map.of(), Map.of(), totalInner, outerIter, config, null);
            }

            // --- Step 16: ALS inner loop ---
            innerConverged = false;
            int innerIter;
            for (innerIter = 0; innerIter < config.maxInnerIterations(); innerIter++)
            {
                if (stopCheck.get())
                {
                    LOG.info("PF optimiser: stopped by request during inner iteration {}", innerIter);
                    return new PfResult(Map.of(), Map.of(), Map.of(), totalInner + innerIter, outerIter, config, null);
                }

                // Step A — Fix PF, solve for T per division
                for (int d = 0; d < nDivs; d++)
                {
                    double sumW = 0, sumWX = 0;
                    for (int idx : divEntryIndexes.get(d))
                    {
                        Entry e = entries.get(idx);
                        double w = entryWeights[idx];
                        double logPfVal = logPf[e.variant()][e.boatOrdinal()];
                        if (Double.isNaN(logPfVal)) continue;
                        sumW += w;
                        sumWX += w * (e.logElapsed() + logPfVal);
                    }
                    if (sumW > 0)
                    {
                        double newLogT = sumWX / sumW;
                        if (!Double.isNaN(newLogT))
                            logT[d] = newLogT;
                    }
                }

                // Step B — Fix T, solve for PF per boat-variant
                double maxDelta = 0;
                for (int v = 0; v < 3; v++)
                {
                    for (int b = 0; b < nBoats; b++)
                    {
                        List<Integer> bEntries = boatVariantEntries[v][b];
                        if (bEntries.isEmpty()) continue;

                        String boatId = ordinalToBoatId[b];
                        double rfW   = boatRfWeight.get(boatId)[v];
                        double rfLog = boatLogRf.get(boatId)[v];           // raw, may be NaN
                        double rfLogForReg = Double.isNaN(rfLog) ? 0.0 : rfLog;

                        double sumW = 0, sumWX = 0;
                        for (int idx : bEntries)
                        {
                            Entry e = entries.get(idx);
                            double w = entryWeights[idx];
                            sumW  += w;
                            sumWX += w * (logT[e.divOrdinal()] - e.logElapsed());
                        }

                        double denom = sumW + config.lambda() * rfW;
                        double numer = sumWX + config.lambda() * rfW * rfLogForReg;

                        // Cross-variant coupling: pull ratio toward this BOAT's RF-implied ratio
                        double mu = config.crossVariantLambda();
                        if (mu > 0 && !Double.isNaN(rfLog))
                        {
                            double[] rfLogs = boatLogRf.get(boatId);
                            for (int v2 = 0; v2 < 3; v2++)
                            {
                                if (v2 == v) continue;
                                double rfLog2 = rfLogs[v2];
                                if (Double.isNaN(rfLog2)) continue;
                                numer += mu * (logPf[v2][b] + (rfLog - rfLog2));
                                denom += mu;
                            }
                        }

                        // Graph-driven cross-variant coupling: pull each variant's PF toward
                        // the FLEET-WIDE conversion-graph prediction of the other variants'
                        // current PFs (Gauss-Seidel — reads current logPf[v2][b] each pass).
                        double muG = config.graphCrossVariantLambda();
                        if (muG > 0 && varConv != null)
                        {
                            BoatDerived bd = boatDerivedMap.get(boatId);
                            Boat boatRec = bd != null ? bd.boat() : null;
                            boolean noSpinDesign = boatRec != null && boatRec.designId() != null
                                && store.isDesignNoSpinnaker(boatRec.designId());
                            double[] rfLogs2 = boatLogRf.get(boatId);
                            for (int v2 = 0; v2 < 3; v2++)
                            {
                                if (v2 == v)
                                    continue;
                                // Cat-rigged designs: spin and nonSpin slots are collapsed
                                // post-hoc in mergePfResults; don't pull them apart here.
                                if (noSpinDesign && ((v == SPIN && v2 == NON_SPIN)
                                    || (v == NON_SPIN && v2 == SPIN)))
                                    continue;
                                VariantConverter c = varConv[v2][v];
                                if (c == null)
                                    continue;
                                // Need some signal on v2 (races or a real RF) to predict from.
                                if (entryCount[v2][b] == 0 && Double.isNaN(rfLogs2[v2]))
                                    continue;
                                double predLogPf = c.slope * logPf[v2][b] + c.intercept;
                                double w = muG * c.weight;
                                numer += w * predLogPf;
                                denom += w;
                            }
                        }
                        // TODO: INVERSE CONVERSION — If we need to invert a VariantConverter
                        //       (e.g., converting from B back to A when we have A's data but need B's),
                        //       the inverse logic should live on Factor, not here. This would provide
                        //       a clean semantic: factor.inverse() returns a new Factor with inverted
                        //       value and appropriate weight handling for the reverse direction.

                        if (denom <= 0) continue; // all weights zero — keep current value
                        double newLogPf = numer / denom;
                        if (Double.isNaN(newLogPf) || Double.isInfinite(newLogPf)) continue;
                        double delta = Math.abs(newLogPf - logPf[v][b]);
                        if (delta > maxDelta)
                            maxDelta = delta;
                        logPf[v][b] = newLogPf;
                    }
                }

                finalMaxDelta = maxDelta;
                if (maxDelta < config.convergenceThreshold())
                {
                    totalInner += innerIter + 1;
                    innerConverged = true;
                    LOG.info("PF optimiser: inner loop converged in {} iterations (outer {}), maxDelta={}",
                        innerIter + 1, outerIter, maxDelta);
                    break;
                }

                if (innerIter == config.maxInnerIterations() - 1)
                {
                    totalInner += config.maxInnerIterations();
                    LOG.info("PF optimiser: inner loop reached max {} iterations (outer {}), maxDelta={}",
                        config.maxInnerIterations(), outerIter, maxDelta);
                }
            }

            // --- Step 17: Reweight ---
            // Recompute T₀ and IQR
            computeT0(entries, divEntryIndexes, logPf, entryWeights, logT, divIqrLog);

            // Recompute entry weights
            double[] oldWeights = Arrays.copyOf(entryWeights, entryWeights.length);
            computeEntryWeights(entries, entryWeights, logPf, logT, divIqrLog, config);

            // Damped update: blend computed weights with previous weights to suppress oscillation.
            // outerDampingFactor=1.0 means fully accept new weights (no damping);
            // outerDampingFactor=0.5 means take half-steps, halving the effective change each cycle.
            double alpha = config.outerDampingFactor();
            if (alpha < 1.0)
                for (int i = 0; i < entryWeights.length; i++)
                    entryWeights[i] = (1.0 - alpha) * oldWeights[i] + alpha * entryWeights[i];

            // Check outer convergence: max weight change
            double maxWeightChange = 0;
            for (int i = 0; i < entryWeights.length; i++)
            {
                double change = Math.abs(entryWeights[i] - oldWeights[i]);
                if (change > maxWeightChange)
                    maxWeightChange = change;
            }
            finalMaxWeightChange = maxWeightChange;
            outerDeltaTrace.add(maxWeightChange);

            if (maxWeightChange < config.outerConvergenceThreshold())
            {
                outerIter++;
                outerConverged = true;
                LOG.info("PF optimiser: outer loop converged in {} iterations, maxWeightChange={}", outerIter, maxWeightChange);
                break;
            }
        }

        // --- Step 19: Output assembly ---
        return assembleResult(config, ordinalToBoatId, ordinalToDivKey, boatLogRf,
            logPf, logT, divIqrLog, entries, entryWeights, entryCount,
            divEntryIndexes, boatVariantEntries, nBoats, nDivs, totalInner, outerIter,
            innerConverged, outerConverged, finalMaxDelta, finalMaxWeightChange,
            List.copyOf(outerDeltaTrace), store, boatDerivedMap, varConv);
    }

    private int determineVariant(Finisher f, Division div, boolean raceForceNonSpin)
    {
        // Check division name for two-handed indicators first (strongest signal)
        String divName = div.name() != null ? div.name().toLowerCase() : "";
        if (divName.contains("2hd") || divName.contains("two-handed") || divName.contains("two handed")
            || divName.contains("double-handed") || divName.contains("double handed")
            || divName.contains("shorthanded") || divName.contains("short-handed")
            || divName.contains("2 handed"))
            return TWO_HANDED;

        // Division name non-spin keywords override per-entry SailSys flag
        if (containsNonSpinKeyword(divName))
            return NON_SPIN;

        // Series-level non-spin override: SailSys sets nonSpinnaker based on certificate type,
        // not race rules — a boat with a spinnaker cert racing in a NS series gets nonSpinnaker=false
        if (raceForceNonSpin)
            return NON_SPIN;

        return f.nonSpinnaker() ? NON_SPIN : SPIN;
    }

    private static boolean containsNonSpinKeyword(String text)
    {
        if (text == null) return false;
        String t = text.toLowerCase();
        return t.contains("non-spinnaker") || t.contains("non spinnaker")
            || t.contains("nonspinnaker") || t.contains("non-spin") || t.contains("non spin");
    }

    private static Factor variantFactor(ReferenceFactors rf, int variant)
    {
        return switch (variant)
        {
            case SPIN -> rf.spin();
            case NON_SPIN -> rf.nonSpin();
            case TWO_HANDED -> rf.twoHanded();
            default -> null;
        };
    }

    /**
     * Compute T₀ per division as weighted median of corrected times in log space,
     * and compute IQR in log space.
     */
    private void computeT0(List<Entry> entries, List<List<Integer>> divEntryIndexes,
                           double[][] logPf, double[] entryWeights,
                           double[] logT, double[] divIqrLog)
    {
        for (int d = 0; d < logT.length; d++)
        {
            List<Integer> dEntries = divEntryIndexes.get(d);
            if (dEntries.isEmpty()) continue;

            // correctedLogTime = logElapsed + logPf (because PF × elapsed = corrected to ref boat pace,
            // but actually: correctedTime = elapsed × PF (PF is the boat's factor),
            // so T₀ = median(elapsed × PF) across entries.
            // In log: logCorrected = logElapsed + logPf
            // But wait — T₀ should satisfy: elapsed ≈ T₀ / PF → log(elapsed) ≈ log(T₀) - log(PF)
            // So: log(T₀) ≈ log(elapsed) + log(PF)
            // Yes, corrected = log(elapsed) + log(PF)

            int n = dEntries.size();
            double[] values = new double[n];
            double[] weights = new double[n];
            double totalWeight = 0;

            for (int i = 0; i < n; i++)
            {
                int idx = dEntries.get(i);
                Entry e = entries.get(idx);
                values[i] = e.logElapsed() + logPf[e.variant()][e.boatOrdinal()];
                weights[i] = entryWeights[idx];
                totalWeight += weights[i];
            }

            // Sort by value, keeping weights aligned
            sortParallel(values, weights, n);

            logT[d] = weightedQuantile(values, weights, totalWeight, n, 0.5);
            double q25 = weightedQuantile(values, weights, totalWeight, n, 0.25);
            double q75 = weightedQuantile(values, weights, totalWeight, n, 0.75);
            divIqrLog[d] = q75 - q25;
        }
    }

    /**
     * Compute entry weights using Cauchy (Lorentzian) down-weighting with asymmetry.
     */
    private void computeEntryWeights(List<Entry> entries, double[] entryWeights,
                                     double[][] logPf, double[] logT, double[] divIqrLog,
                                     PfConfig config)
    {
        for (int i = 0; i < entries.size(); i++)
        {
            Entry e = entries.get(i);
            double residual = e.logElapsed() + logPf[e.variant()][e.boatOrdinal()] - logT[e.divOrdinal()];
            double iqr = divIqrLog[e.divOrdinal()];
            if (iqr <= 0) iqr = 0.01; // prevent division by zero

            double effectiveDeviation = residual < 0
                ? Math.abs(residual) * config.asymmetryFactor()
                : Math.abs(residual);

            double scale = config.outlierK() * iqr;
            double ratio = effectiveDeviation / scale;
            entryWeights[i] = e.refWeight() / (1.0 + ratio * ratio);
        }
    }

    private PfResult assembleResult(PfConfig config,
                                     String[] ordinalToBoatId, DivisionKey[] ordinalToDivKey,
                                    Map<String, double[]> boatLogRf,
                                     double[][] logPf, double[] logT, double[] divIqrLog,
                                     List<Entry> entries, double[] entryWeights,
                                     int[][] entryCount,
                                     List<List<Integer>> divEntryIndexes,
                                     List<Integer>[][] boatVariantEntries,
                                     int nBoats, int nDivs,
                                     int totalInner, int outerIter,
                                     boolean innerConverged, boolean outerConverged,
                                     double finalMaxDelta, double finalMaxWeightChange,
                                     List<Double> outerDeltaTrace,
                                    DataStore store, Map<String, BoatDerived> boatDerivedMap,
                                    VariantConverter[][] varConv)
    {
        // Boat PFs
        Map<String, BoatPf> boatPfs = new LinkedHashMap<>();
        Map<String, List<EntryResidual>> residualsByBoatId = new LinkedHashMap<>();

        for (int b = 0; b < nBoats; b++)
        {
            String boatId = ordinalToBoatId[b];
            double[] rfLog = boatLogRf.get(boatId);

            BoatDerived bd = boatDerivedMap.get(boatId);
            ReferenceFactors rf = bd != null ? bd.referenceFactors() : null;

            Factor[] pfFactors = new Factor[3];
            double[] refDeltas = new double[3];
            int[] raceCounts = new int[3];

            for (int v = 0; v < 3; v++)
            {
                raceCounts[v] = entryCount[v][b];
                Factor rfFactor = rf != null ? variantFactor(rf, v) : null;

                if (raceCounts[v] > 0)
                {
                    double pfVal = Math.exp(logPf[v][b]);
                    if (Double.isNaN(pfVal) || Double.isInfinite(pfVal) || pfVal <= 0)
                    {
                        // Fall back to RF if solver produced invalid value
                        if (rfFactor != null)
                        {
                            pfFactors[v] = rfFactor;
                            refDeltas[v] = 0.0;
                        }
                    }
                    else
                    {
                        // Compute confidence from total weighted entry count
                        double totalEntryWeight = 0;
                        for (int idx : boatVariantEntries[v][b])
                            totalEntryWeight += entryWeights[idx];
                        double confidence = Math.min(1.0, totalEntryWeight / 5.0);

                        pfFactors[v] = new Factor(pfVal, confidence);
                        refDeltas[v] = Double.isNaN(rfLog[v]) ? 0.0 : logPf[v][b] - rfLog[v];
                    }
                }
                else if (rfFactor != null)
                {
                    // No races for this variant — PF takes RF's value, but the weight must
                    // reflect the absence of direct observations of this boat in variant v.
                    // The boat's RF may be high-confidence (inherited from a well-evidenced
                    // design or aggregated via cross-variant blend in ReferenceNetworkBuilder),
                    // but for THIS boat we have no race data for variant v. Cap the PF weight
                    // by the cross-variant graph's inference confidence: for each other variant
                    // v2 in which the boat has races, build a Factor whose weight is the boat's
                    // race-derived confidence in v2 (totalWeight/5) times the v2→v conversion
                    // confidence, then Factor.aggregate across source variants and cap by
                    // rfFactor.weight(). When no graph inference is available, fall back to
                    // config.noRaceFallbackWeight() (default 0.2).
                    double newWeight;
                    List<Factor> inferences = null;
                    if (varConv != null)
                    {
                        for (int v2 = 0; v2 < 3; v2++)
                        {
                            if (v2 == v)
                                continue;
                            VariantConverter conv = varConv[v2][v];
                            if (conv == null)
                                continue;
                            double totalV2EntryWeight = 0;
                            for (int idx : boatVariantEntries[v2][b])
                            {
                                totalV2EntryWeight += entryWeights[idx];
                            }
                            if (totalV2EntryWeight <= 0)
                                continue;
                            double raceConfidence = Math.min(1.0, totalV2EntryWeight / 5.0);
                            double w = raceConfidence * conv.weight();
                            if (w <= 0)
                                continue;
                            if (inferences == null)
                                inferences = new ArrayList<>(2);
                            inferences.add(new Factor(rfFactor.value(), w));
                        }
                    }
                    if (inferences != null && !inferences.isEmpty())
                    {
                        Factor agg = inferences.size() == 1
                            ? inferences.get(0)
                            : Factor.aggregate(inferences.toArray(new Factor[0]));
                        newWeight = Math.min(rfFactor.weight(), agg.weight());
                    }
                    else
                    {
                        newWeight = Math.min(rfFactor.weight(), config.noRaceFallbackWeight());
                    }
                    pfFactors[v] = new Factor(rfFactor.value(), newWeight);
                    refDeltas[v] = 0.0;
                }
                // else null — no RF and no races
            }

            boatPfs.put(boatId, new BoatPf(
                pfFactors[SPIN], pfFactors[NON_SPIN], pfFactors[TWO_HANDED],
                refDeltas[SPIN], refDeltas[NON_SPIN], refDeltas[TWO_HANDED],
                raceCounts[SPIN], raceCounts[NON_SPIN], raceCounts[TWO_HANDED]));

            // Collect residuals for this boat
            List<EntryResidual> residuals = new ArrayList<>();
            for (int v = 0; v < 3; v++)
            {
                for (int idx : boatVariantEntries[v][b])
                {
                    Entry e = entries.get(idx);
                    double residual = e.logElapsed() + logPf[v][b] - logT[e.divOrdinal()];
                    residuals.add(new EntryResidual(
                        e.raceId(), e.divisionName(), e.raceDate(),
                        v == NON_SPIN, v == TWO_HANDED, residual, entryWeights[idx]));
                }
            }
            if (!residuals.isEmpty())
                residualsByBoatId.put(boatId, List.copyOf(residuals));
        }

        // Division PFs
        Map<String, List<DivisionPf>> divisionPfsByRaceId = new LinkedHashMap<>();
        for (int d = 0; d < nDivs; d++)
        {
            DivisionKey dk = ordinalToDivKey[d];
            Race race = store.races().get(dk.raceId());
            if (race == null || race.divisions() == null) continue;
            Division div = race.divisions().get(dk.divisionIndex());

            // Compute division weight: totalRefWeight / (1 + dispersion²)
            double totalRefWeight = 0;
            for (int idx : divEntryIndexes.get(d))
                totalRefWeight += entries.get(idx).refWeight();
            double t0 = Math.exp(logT[d]);
            double iqr = divIqrLog[d];
            double dispersion = t0 > 0 ? iqr : 0;  // IQR is already in log space, so it's a ratio
            double divWeight = totalRefWeight / (1.0 + dispersion * dispersion);

            DivisionPf dh = new DivisionPf(div.name(), t0, dispersion, divWeight);
            divisionPfsByRaceId.computeIfAbsent(dk.raceId(), k -> new ArrayList<>()).add(dh);
        }

        // Make lists immutable
        divisionPfsByRaceId.replaceAll((k, v) -> List.copyOf(v));

        // --- Compute quality metrics ---

        // Residual statistics: collect residuals for all entries
        int entryCount2 = entries.size();
        double[] signedResiduals = new double[entryCount2];
        for (int i = 0; i < entryCount2; i++)
        {
            Entry e = entries.get(i);
            signedResiduals[i] = e.logElapsed() + logPf[e.variant()][e.boatOrdinal()] - logT[e.divOrdinal()];
        }
        // Median |residual| and P95 |residual| from sorted absolute values
        double[] absResiduals = new double[entryCount2];
        for (int i = 0; i < entryCount2; i++)
            absResiduals[i] = Math.abs(signedResiduals[i]);
        Arrays.sort(absResiduals);
        double medianResidual = simpleQuantile(absResiduals, 0.5);
        double pct95Residual = simpleQuantile(absResiduals, 0.95);
        // IQR from signed residuals (spread of the distribution)
        Arrays.sort(signedResiduals);
        double q25Signed = simpleQuantile(signedResiduals, 0.25);
        double q75Signed = simpleQuantile(signedResiduals, 0.75);
        double iqrResidual = q75Signed - q25Signed;

        // Down-weighted entries: weight < 50% of initial refWeight
        int downWeightedEntries = 0;
        for (int i = 0; i < entryCount2; i++)
        {
            Entry e = entries.get(i);
            if (entryWeights[i] < 0.5 * e.refWeight())
                downWeightedEntries++;
        }

        // High-dispersion divisions: IQR > 0.10
        int highDispersionDivisions = 0;
        for (int d = 0; d < nDivs; d++)
        {
            if (divIqrLog[d] > 0.10)
                highDispersionDivisions++;
        }

        // Median boat confidence: median of spin PF weights for boats with spin PF
        List<Double> boatConfidences = new ArrayList<>();
        for (var entry : boatPfs.values())
        {
            if (entry.spin() != null && !Double.isNaN(entry.spin().weight()))
                boatConfidences.add(entry.spin().weight());
        }
        double medianBoatConfidence = 0;
        if (!boatConfidences.isEmpty())
        {
            double[] confValues = boatConfidences.stream().mapToDouble(Double::doubleValue).toArray();
            Arrays.sort(confValues);
            medianBoatConfidence = simpleQuantile(confValues, 0.5);
        }

        PfQuality quality = new PfQuality(
            boatPfs.size(), entryCount2, nDivs,
            totalInner, outerIter,
            innerConverged, outerConverged,
            finalMaxDelta, finalMaxWeightChange,
            medianResidual, iqrResidual, pct95Residual,
            downWeightedEntries, highDispersionDivisions,
            medianBoatConfidence, outerDeltaTrace, config);

        LOG.info("PF optimiser: complete. {} boats with PF, {} races with division PF",
            boatPfs.size(), divisionPfsByRaceId.size());

        return new PfResult(
            Map.copyOf(boatPfs),
            Map.copyOf(divisionPfsByRaceId),
            Map.copyOf(residualsByBoatId),
            totalInner, outerIter, config, quality);
    }

    // --- Utility methods ---

    /**
     * Simple insertion sort on values, keeping weights aligned. Fine for small arrays.
     */
    private static void sortParallel(double[] values, double[] weights, int n)
    {
        for (int i = 1; i < n; i++)
        {
            double vKey = values[i];
            double wKey = weights[i];
            int j = i - 1;
            while (j >= 0 && values[j] > vKey)
            {
                values[j + 1] = values[j];
                weights[j + 1] = weights[j];
                j--;
            }
            values[j + 1] = vKey;
            weights[j + 1] = wKey;
        }
    }

    /**
     * Simple quantile with linear interpolation on a pre-sorted array (uniform weights).
     */
    static double simpleQuantile(double[] sorted, double quantile)
    {
        int n = sorted.length;
        if (n == 0) return 0;
        if (n == 1) return sorted[0];
        double pos = quantile * (n - 1);
        int lo = (int) Math.floor(pos);
        int hi = Math.min(lo + 1, n - 1);
        double frac = pos - lo;
        return sorted[lo] + frac * (sorted[hi] - sorted[lo]);
    }

    /**
     * Weighted quantile with linear interpolation between straddling entries.
     * Assumes values and weights are sorted by value.
     */
    static double weightedQuantile(double[] values, double[] weights,
                                           double totalWeight, int n, double quantile)
    {
        if (n == 1) return values[0];

        double target = quantile * totalWeight;
        double cumulative = 0;
        for (int i = 0; i < n; i++)
        {
            double prev = cumulative;
            cumulative += weights[i];
            if (cumulative >= target)
            {
                if (i == 0) return values[0];
                // Linear interpolation
                double fraction = (target - prev) / weights[i];
                return values[i - 1] + fraction * (values[i] - values[i - 1]);
            }
        }
        return values[n - 1];
    }
}
