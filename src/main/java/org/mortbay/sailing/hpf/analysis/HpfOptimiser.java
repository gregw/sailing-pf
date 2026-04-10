package org.mortbay.sailing.hpf.analysis;

import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Factor;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * HPF optimiser: weighted alternating least squares in log space.
 * <p>
 * Produces two unknowns per iteration: HPF per boat-variant, and reference time T
 * per division. A regularisation term pulls each boat's HPF toward its reference factor.
 * An outer loop reweights entries using residual-based outlier detection with an
 * asymmetry principle (fast outliers penalised more than slow ones).
 * <p>
 * The race unit is the Division — one T₀ per division. All boats in a division
 * sailed the same course. Mixed spin/nonSpin within a division: each boat uses its
 * own variant's HPF for correction; the shared T₀ links the variants.
 */
public class HpfOptimiser
{
    private static final Logger LOG = LoggerFactory.getLogger(HpfOptimiser.class);

    // --- Working data structures ---

    private record DivisionKey(String raceId, int divisionIndex) {}

    private record Entry(
        int boatOrdinal,        // index into logHpf arrays
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

    public HpfResult optimise(
        DataStore store,
        Map<String, BoatDerived> boatDerivedMap,
        HpfConfig config,
        Supplier<Boolean> stopCheck)
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
                    if (bd == null || bd.referenceFactors() == null) continue;

                    ReferenceFactors rf = bd.referenceFactors();
                    int variant = determineVariant(f, div, raceForceNonSpin);
                    Factor rfFactor = variantFactor(rf, variant);
                    // Allow rfFactor.weight() == 0: the Step-B formula degenerates cleanly to a
                    // pure race-derived HPF (no regularisation toward RF) when rfW = 0, so these
                    // boats still contribute to division time calibration and receive an HPF.
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
            LOG.warn("HPF optimiser: no qualifying data (boats={}, divs={}, entries={})", nBoats, nDivs, entries.size());
            return new HpfResult(Map.of(), Map.of(), Map.of(), 0, 0, config, null);
        }

        LOG.info("HPF optimiser: {} boats, {} divisions, {} entries", nBoats, nDivs, entries.size());

        // Build reverse map: ordinal → boatId
        String[] ordinalToBoatId = new String[nBoats];
        for (var e : boatOrdinals.entrySet())
            ordinalToBoatId[e.getValue()] = e.getKey();

        // Build reverse map: ordinal → DivisionKey
        DivisionKey[] ordinalToDivKey = new DivisionKey[nDivs];
        for (var e : divOrdinals.entrySet())
            ordinalToDivKey[e.getValue()] = e.getKey();

        // Working arrays — 3 variants per boat
        double[][] logHpf = new double[3][nBoats];   // [variant][boatOrd]
        double[] logT = new double[nDivs];
        double[] entryWeights = new double[entries.size()];
        Arrays.fill(entryWeights, 1.0);

        // Initialise logHpf from RF values
        for (int b = 0; b < nBoats; b++)
        {
            String boatId = ordinalToBoatId[b];
            double[] lr = boatLogRf.get(boatId);
            for (int v = 0; v < 3; v++)
                logHpf[v][b] = Double.isNaN(lr[v]) ? 0.0 : lr[v];
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

        // --- Step 13: Initial T₀ per division (weighted median of corrected times) ---
        double[] divIqrLog = new double[nDivs];
        computeT0(entries, divEntryIndexes, logHpf, entryWeights, logT, divIqrLog);

        // --- Step 15: Initial entry weights ---
        computeEntryWeights(entries, entryWeights, logHpf, logT, divIqrLog, config);

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
                LOG.info("HPF optimiser: stopped by request after {} outer iterations", outerIter);
                return new HpfResult(Map.of(), Map.of(), Map.of(), totalInner, outerIter, config, null);
            }

            // --- Step 16: ALS inner loop ---
            innerConverged = false;
            int innerIter;
            for (innerIter = 0; innerIter < config.maxInnerIterations(); innerIter++)
            {
                if (stopCheck.get())
                {
                    LOG.info("HPF optimiser: stopped by request during inner iteration {}", innerIter);
                    return new HpfResult(Map.of(), Map.of(), Map.of(), totalInner + innerIter, outerIter, config, null);
                }

                // Step A — Fix HPF, solve for T per division
                for (int d = 0; d < nDivs; d++)
                {
                    double sumW = 0, sumWX = 0;
                    for (int idx : divEntryIndexes.get(d))
                    {
                        Entry e = entries.get(idx);
                        double w = entryWeights[idx];
                        double logHpfVal = logHpf[e.variant()][e.boatOrdinal()];
                        if (Double.isNaN(logHpfVal)) continue;
                        sumW += w;
                        sumWX += w * (e.logElapsed() + logHpfVal);
                    }
                    if (sumW > 0)
                    {
                        double newLogT = sumWX / sumW;
                        if (!Double.isNaN(newLogT))
                            logT[d] = newLogT;
                    }
                }

                // Step B — Fix T, solve for HPF per boat-variant
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

                        // Cross-variant coupling: pull ratio toward RF-implied ratio
                        double mu = config.crossVariantLambda();
                        if (mu > 0 && !Double.isNaN(rfLog))
                        {
                            double[] rfLogs = boatLogRf.get(boatId);
                            for (int v2 = 0; v2 < 3; v2++)
                            {
                                if (v2 == v) continue;
                                double rfLog2 = rfLogs[v2];
                                if (Double.isNaN(rfLog2)) continue;
                                numer += mu * (logHpf[v2][b] + (rfLog - rfLog2));
                                denom += mu;
                            }
                        }

                        if (denom <= 0) continue; // all weights zero — keep current value
                        double newLogHpf = numer / denom;
                        if (Double.isNaN(newLogHpf) || Double.isInfinite(newLogHpf)) continue;
                        double delta = Math.abs(newLogHpf - logHpf[v][b]);
                        if (delta > maxDelta)
                            maxDelta = delta;
                        logHpf[v][b] = newLogHpf;
                    }
                }

                finalMaxDelta = maxDelta;
                if (maxDelta < config.convergenceThreshold())
                {
                    totalInner += innerIter + 1;
                    innerConverged = true;
                    LOG.info("HPF optimiser: inner loop converged in {} iterations (outer {}), maxDelta={}",
                        innerIter + 1, outerIter, maxDelta);
                    break;
                }

                if (innerIter == config.maxInnerIterations() - 1)
                {
                    totalInner += config.maxInnerIterations();
                    LOG.info("HPF optimiser: inner loop reached max {} iterations (outer {}), maxDelta={}",
                        config.maxInnerIterations(), outerIter, maxDelta);
                }
            }

            // --- Step 17: Reweight ---
            // Recompute T₀ and IQR
            computeT0(entries, divEntryIndexes, logHpf, entryWeights, logT, divIqrLog);

            // Recompute entry weights
            double[] oldWeights = Arrays.copyOf(entryWeights, entryWeights.length);
            computeEntryWeights(entries, entryWeights, logHpf, logT, divIqrLog, config);

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
                LOG.info("HPF optimiser: outer loop converged in {} iterations, maxWeightChange={}", outerIter, maxWeightChange);
                break;
            }
        }

        // --- Step 19: Output assembly ---
        return assembleResult(config, ordinalToBoatId, ordinalToDivKey, boatLogRf, boatRfWeight,
            logHpf, logT, divIqrLog, entries, entryWeights, entryCount,
            divEntryIndexes, boatVariantEntries, nBoats, nDivs, totalInner, outerIter,
            innerConverged, outerConverged, finalMaxDelta, finalMaxWeightChange,
            List.copyOf(outerDeltaTrace), store, boatDerivedMap);
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
                           double[][] logHpf, double[] entryWeights,
                           double[] logT, double[] divIqrLog)
    {
        for (int d = 0; d < logT.length; d++)
        {
            List<Integer> dEntries = divEntryIndexes.get(d);
            if (dEntries.isEmpty()) continue;

            // correctedLogTime = logElapsed + logHpf (because HPF × elapsed = corrected to ref boat pace,
            // but actually: correctedTime = elapsed × HPF (HPF is the boat's factor),
            // so T₀ = median(elapsed × HPF) across entries.
            // In log: logCorrected = logElapsed + logHpf
            // But wait — T₀ should satisfy: elapsed ≈ T₀ / HPF → log(elapsed) ≈ log(T₀) - log(HPF)
            // So: log(T₀) ≈ log(elapsed) + log(HPF)
            // Yes, corrected = log(elapsed) + log(HPF)

            int n = dEntries.size();
            double[] values = new double[n];
            double[] weights = new double[n];
            double totalWeight = 0;

            for (int i = 0; i < n; i++)
            {
                int idx = dEntries.get(i);
                Entry e = entries.get(idx);
                values[i] = e.logElapsed() + logHpf[e.variant()][e.boatOrdinal()];
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
                                     double[][] logHpf, double[] logT, double[] divIqrLog,
                                     HpfConfig config)
    {
        for (int i = 0; i < entries.size(); i++)
        {
            Entry e = entries.get(i);
            double residual = e.logElapsed() + logHpf[e.variant()][e.boatOrdinal()] - logT[e.divOrdinal()];
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

    private HpfResult assembleResult(HpfConfig config,
                                     String[] ordinalToBoatId, DivisionKey[] ordinalToDivKey,
                                     Map<String, double[]> boatLogRf, Map<String, double[]> boatRfWeight,
                                     double[][] logHpf, double[] logT, double[] divIqrLog,
                                     List<Entry> entries, double[] entryWeights,
                                     int[][] entryCount,
                                     List<List<Integer>> divEntryIndexes,
                                     List<Integer>[][] boatVariantEntries,
                                     int nBoats, int nDivs,
                                     int totalInner, int outerIter,
                                     boolean innerConverged, boolean outerConverged,
                                     double finalMaxDelta, double finalMaxWeightChange,
                                     List<Double> outerDeltaTrace,
                                     DataStore store, Map<String, BoatDerived> boatDerivedMap)
    {
        // Boat HPFs
        Map<String, BoatHpf> boatHpfs = new LinkedHashMap<>();
        Map<String, List<EntryResidual>> residualsByBoatId = new LinkedHashMap<>();

        for (int b = 0; b < nBoats; b++)
        {
            String boatId = ordinalToBoatId[b];
            double[] rfLog = boatLogRf.get(boatId);
            double[] rfW = boatRfWeight.get(boatId);

            BoatDerived bd = boatDerivedMap.get(boatId);
            ReferenceFactors rf = bd != null ? bd.referenceFactors() : null;

            Factor[] hpfFactors = new Factor[3];
            double[] refDeltas = new double[3];
            int[] raceCounts = new int[3];

            for (int v = 0; v < 3; v++)
            {
                raceCounts[v] = entryCount[v][b];
                Factor rfFactor = rf != null ? variantFactor(rf, v) : null;

                if (raceCounts[v] > 0)
                {
                    double hpfVal = Math.exp(logHpf[v][b]);
                    if (Double.isNaN(hpfVal) || Double.isInfinite(hpfVal) || hpfVal <= 0)
                    {
                        // Fall back to RF if solver produced invalid value
                        if (rfFactor != null)
                        {
                            hpfFactors[v] = rfFactor;
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

                        hpfFactors[v] = new Factor(hpfVal, confidence);
                        refDeltas[v] = Double.isNaN(rfLog[v]) ? 0.0 : logHpf[v][b] - rfLog[v];
                    }
                }
                else if (rfFactor != null)
                {
                    // HPF == RF for variants with no races
                    hpfFactors[v] = rfFactor;
                    refDeltas[v] = 0.0;
                }
                // else null — no RF and no races
            }

            boatHpfs.put(boatId, new BoatHpf(
                hpfFactors[SPIN], hpfFactors[NON_SPIN], hpfFactors[TWO_HANDED],
                refDeltas[SPIN], refDeltas[NON_SPIN], refDeltas[TWO_HANDED],
                raceCounts[SPIN], raceCounts[NON_SPIN], raceCounts[TWO_HANDED]));

            // Collect residuals for this boat
            List<EntryResidual> residuals = new ArrayList<>();
            for (int v = 0; v < 3; v++)
            {
                for (int idx : boatVariantEntries[v][b])
                {
                    Entry e = entries.get(idx);
                    double residual = e.logElapsed() + logHpf[v][b] - logT[e.divOrdinal()];
                    residuals.add(new EntryResidual(
                        e.raceId(), e.divisionName(), e.raceDate(),
                        v == NON_SPIN, v == TWO_HANDED, residual, entryWeights[idx]));
                }
            }
            if (!residuals.isEmpty())
                residualsByBoatId.put(boatId, List.copyOf(residuals));
        }

        // Division HPFs
        Map<String, List<DivisionHpf>> divisionHpfsByRaceId = new LinkedHashMap<>();
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

            DivisionHpf dh = new DivisionHpf(div.name(), t0, dispersion, divWeight);
            divisionHpfsByRaceId.computeIfAbsent(dk.raceId(), k -> new ArrayList<>()).add(dh);
        }

        // Make lists immutable
        divisionHpfsByRaceId.replaceAll((k, v) -> List.copyOf(v));

        // --- Compute quality metrics ---

        // Residual statistics: collect residuals for all entries
        int entryCount2 = entries.size();
        double[] signedResiduals = new double[entryCount2];
        for (int i = 0; i < entryCount2; i++)
        {
            Entry e = entries.get(i);
            signedResiduals[i] = e.logElapsed() + logHpf[e.variant()][e.boatOrdinal()] - logT[e.divOrdinal()];
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

        // Median boat confidence: median of spin HPF weights for boats with spin HPF
        List<Double> boatConfidences = new ArrayList<>();
        for (var entry : boatHpfs.values())
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

        HpfQuality quality = new HpfQuality(
            boatHpfs.size(), entryCount2, nDivs,
            totalInner, outerIter,
            innerConverged, outerConverged,
            finalMaxDelta, finalMaxWeightChange,
            medianResidual, iqrResidual, pct95Residual,
            downWeightedEntries, highDispersionDivisions,
            medianBoatConfidence, outerDeltaTrace, config);

        LOG.info("HPF optimiser: complete. {} boats with HPF, {} races with division HPF",
            boatHpfs.size(), divisionHpfsByRaceId.size());

        return new HpfResult(
            Map.copyOf(boatHpfs),
            Map.copyOf(divisionHpfsByRaceId),
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
