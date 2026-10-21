package org.mortbay.sailing.pf.analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PF optimiser by simulated annealing: an alternative to {@link PfOptimiser} (ALS) that directly
 * minimises the {@link PfScore} energy — the weighted variance of corrected times within each
 * division, plus λ·rfW·(log PF − log RF)² pulling each boat toward its reference factor.
 * <p>
 * Each move nudges a random subset of boat-variant PFs by Gaussian steps in log space and is
 * kept by the Metropolis rule ({@code exp(−ΔE/T)}). A division's par time T₀ is not a free
 * variable: the weighted mean of its corrected times is the exact minimiser of its variance, so
 * per-division running sums Σw, Σwx, Σwx² give ΔE in O(1) per moved entry. The starting temperature is
 * calibrated from single-PF moves, it cools geometrically, and the step size adapts each stage to
 * keep about 30–50% of moves accepted. Outlier and fast-boat weights depend on the
 * residuals, so they are frozen within a stage (keeping ΔE consistent) and refreshed between
 * stages. Each stage ends with a full {@link PfScore}; the lowest-scoring PFs seen are the result.
 * <p>
 * Starts from the boats' RFs, as ALS does. Cross-variant coupling (ALS's graph and RF-ratio
 * pulls) is not part of the objective.
 */
public class PfAnnealer
{
    private static final Logger LOG = LoggerFactory.getLogger(PfAnnealer.class);

    /** Moves sampled from the start to set the initial temperature. */
    private static final int CALIBRATION_MOVES = 200;
    /** Initial temperature is set so an average uphill single-PF move is accepted with this probability. */
    private static final double INITIAL_UPHILL_ACCEPTANCE = 0.8;
    /** The step size adapts after each stage to keep the acceptance rate within this band. */
    private static final double ACCEPTANCE_LOW = 0.3;
    private static final double ACCEPTANCE_HIGH = 0.5;
    private static final double STEP_ADAPT = 1.25;
    /** Acceptance below this for {@link #FROZEN_STAGES} stages in a row ends the run. */
    private static final double FROZEN_ACCEPTANCE = 0.01;
    private static final int FROZEN_STAGES = 3;

    /** The PFs (as an ordinary {@link PfResult}, scored in its quality) and how the run went. */
    public record Result(PfResult pfResult, PfAnnealStats stats) {}

    public Result anneal(
        DataStore store,
        Map<String, BoatDerived> boatDerivedMap,
        PfConfig config,
        PfAnnealConfig annealConfig,
        Supplier<Boolean> stopCheck,
        ConversionGraph graph,
        int targetYear,
        DuplicateRaces duplicates)
    {
        long startNanos = System.nanoTime();
        PfProblem p = PfProblem.build(store, boatDerivedMap, config, duplicates);
        if (p.isEmpty())
        {
            LOG.warn("PF annealer: no qualifying data (boats={}, divs={}, entries={})",
                p.nBoats, p.nDivs, p.entries.size());
            return new Result(new PfResult(Map.of(), Map.of(), Map.of(), 0, 0, config, null), null);
        }
        LOG.info("PF annealer: {} boats, {} divisions, {} entries", p.nBoats, p.nDivs, p.entries.size());

        State s = new State(p, config, annealConfig);
        double[][] best = copy(s.logPf);
        PfScore bestScore = PfScore.of(p, s.logPf, config);
        int bestStage = 0;
        double startEnergy = bestScore.energy();

        // --- Initial temperature: from the mean uphill ΔE of single-PF trial moves. A move's ΔE
        // grows with the number of PFs it perturbs, but the equilibrium spread of each PF at a
        // temperature does not, so T is calibrated per PF and the step size adapts instead. ---
        double sumUp = 0;
        int nUp = 0;
        for (int i = 0; i < CALIBRATION_MOVES && s.active.length > 0; i++)
        {
            double dE = s.move(annealConfig.stepSize(), 1);
            s.revert();
            if (dE > 0)
            {
                sumUp += dE;
                nUp++;
            }
        }
        double t0 = nUp > 0 ? -(sumUp / nUp) / Math.log(INITIAL_UPHILL_ACCEPTANCE) : 1.0e-12;
        double minT = t0 * annealConfig.minTemperatureRatio();
        LOG.info("PF annealer: {} active boat-variants, {} per move, initial T={}, start energy={}",
            s.active.length, s.perMove, t0, bestScore.energy());

        // --- Cooling schedule ---
        double t = t0;
        // Perturbing n PFs at once adds n uphill terms, so start each PF's step at 1/√n
        double sigma = annealConfig.stepSize() / Math.sqrt(Math.max(1, s.perMove));
        int stage = 0;
        long moves = 0;
        long accepted = 0;
        int lowAcceptStages = 0;
        String stopReason = "max stages";
        List<Double> acceptanceTrace = new ArrayList<>();
        List<Double> energyTrace = new ArrayList<>();
        while (stage < annealConfig.maxStages() && s.active.length > 0)
        {
            if (stopCheck.get())
            {
                LOG.info("PF annealer: stopped by request after {} stages", stage);
                return new Result(new PfResult(Map.of(), Map.of(), Map.of(), (int)Math.min(moves, Integer.MAX_VALUE),
                    stage, config, null), null);
            }

            int stageAccepted = 0;
            for (int m = 0; m < annealConfig.movesPerTemperature(); m++)
            {
                double dE = s.move(sigma, s.perMove);
                if (dE <= 0 || s.rng.nextDouble() < Math.exp(-dE / t))
                    stageAccepted++;
                else
                    s.revert();
            }
            stage++;
            moves += annealConfig.movesPerTemperature();
            accepted += stageAccepted;
            double acceptance = (double)stageAccepted / annealConfig.movesPerTemperature();
            acceptanceTrace.add(acceptance);
            if (acceptance > ACCEPTANCE_HIGH)
                sigma *= STEP_ADAPT;
            else if (acceptance < ACCEPTANCE_LOW)
                sigma /= STEP_ADAPT;

            s.refreshWeights();
            PfScore score = PfScore.of(p, s.logPf, config);
            energyTrace.add(score.energy());
            if (score.energy() < bestScore.energy())
            {
                bestScore = score;
                bestStage = stage;
                for (int v = 0; v < 3; v++)
                    System.arraycopy(s.logPf[v], 0, best[v], 0, p.nBoats);
            }

            t *= annealConfig.coolingRate();
            if (t < minT)
            {
                stopReason = "cooled";
                break;
            }
            lowAcceptStages = acceptance < FROZEN_ACCEPTANCE ? lowAcceptStages + 1 : 0;
            if (lowAcceptStages >= FROZEN_STAGES)
            {
                stopReason = "frozen";
                break;
            }
        }

        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
        LOG.info("PF annealer: {} after {} stages, {} moves ({} accepted) in {} ms; best energy {} at stage {} (start {})",
            stopReason, stage, moves, accepted, elapsedMillis, bestScore.energy(), bestStage, startEnergy);

        PfAnnealStats stats = new PfAnnealStats(stage, moves, accepted, s.active.length, s.perMove,
            t0, t, bestStage, stopReason, List.copyOf(acceptanceTrace), List.copyOf(energyTrace),
            elapsedMillis, annealConfig);

        // --- Output: the same assembly as ALS, so the result reads like any PfResult ---
        // Entry weights without age decay (assembleResult applies it) for confidence and residuals.
        double[] refWeights = new double[p.entries.size()];
        for (int i = 0; i < refWeights.length; i++)
            refWeights[i] = p.entries.get(i).refWeight();
        double[] entryWeights = PfScore.entryWeights(p, best, config, refWeights);
        double[] logT = new double[p.nDivs];
        double[] divIqrLog = new double[p.nDivs];
        PfOptimiser.computeT0(p.entries, p.divEntryIndexes, best, entryWeights, logT, divIqrLog);
        PfOptimiser.VariantConverter[][] varConv = (graph != null && targetYear > 0)
            ? PfOptimiser.buildVariantConverters(graph, targetYear)
            : null;
        boolean converged = !"max stages".equals(stopReason);
        PfResult result = new PfOptimiser().assembleResult(config, p.ordinalToBoatId, p.ordinalToDivKey, p.boatLogRf,
            best, logT, divIqrLog, p.entries, entryWeights, p.ageDecay, p.entryCount,
            p.divEntryIndexes, p.boatVariantEntries, p.nBoats, p.nDivs,
            (int)Math.min(moves, Integer.MAX_VALUE), stage, converged, converged,
            0, 0, List.of(), 0, List.of(),
            store, boatDerivedMap, varConv, bestScore);
        return new Result(result, stats);
    }

    private static double[][] copy(double[][] a)
    {
        double[][] c = new double[a.length][];
        for (int i = 0; i < a.length; i++)
            c[i] = a[i].clone();
        return c;
    }

    /**
     * The annealer's working state: current PFs, frozen entry weights and per-division running
     * sums, plus what the last move changed so it can be reverted.
     */
    private static final class State
    {
        final PfProblem p;
        final PfConfig config;
        final Random rng;
        final double[][] logPf;
        final double[] baseWeights;
        final double[] logElapsed;
        final int[] divOrd;
        final int[][][] bvAll;          // [variant][boat] → all its entries, reference-only ones too
        final double[][] rfLog;         // [variant][boat], NaN if none
        final double[][] rfW;           // [variant][boat]
        final int[] active;             // movable boat-variants, encoded v * nBoats + b
        final int perMove;

        // Per-division sums of w, w·x, w·x², with x = corrected time − offset (centred for precision)
        double[] w;
        final double[] offset;
        final double[] s0;
        final double[] s1;
        final double[] s2;

        // Undo record for the last move
        final int[] moved;
        final double[] movedOld;
        final int[] touched;
        final int[] touchStamp;
        final double[] savedS1;
        final double[] savedS2;
        int nTouched;
        int nMoved;
        int stamp;

        State(PfProblem p, PfConfig config, PfAnnealConfig annealConfig)
        {
            this.p = p;
            this.config = config;
            this.rng = new Random(annealConfig.seed());
            int nBoats = p.nBoats;
            int nEntries = p.entries.size();
            logPf = p.initialLogPf();
            baseWeights = PfScore.baseWeights(p);

            logElapsed = new double[nEntries];
            divOrd = new int[nEntries];
            int[][] counts = new int[3][nBoats];
            for (int i = 0; i < nEntries; i++)
            {
                PfProblem.Entry e = p.entries.get(i);
                logElapsed[i] = e.logElapsed();
                divOrd[i] = e.divOrdinal();
                counts[e.variant()][e.boatOrdinal()]++;
            }
            bvAll = new int[3][nBoats][];
            for (int v = 0; v < 3; v++)
                for (int b = 0; b < nBoats; b++)
                    bvAll[v][b] = new int[counts[v][b]];
            int[][] fill = new int[3][nBoats];
            for (int i = 0; i < nEntries; i++)
            {
                PfProblem.Entry e = p.entries.get(i);
                bvAll[e.variant()][e.boatOrdinal()][fill[e.variant()][e.boatOrdinal()]++] = i;
            }

            rfLog = new double[3][nBoats];
            rfW = new double[3][nBoats];
            for (int b = 0; b < nBoats; b++)
            {
                double[] lr = p.boatLogRf.get(p.ordinalToBoatId[b]);
                double[] lw = p.boatRfWeight.get(p.ordinalToBoatId[b]);
                for (int v = 0; v < 3; v++)
                {
                    rfLog[v][b] = lr[v];
                    rfW[v][b] = lw[v];
                }
            }

            // Movable: a boat-variant with results of its own that carry weight, or an RF to pull
            // toward. Without either nothing constrains its PF, so it keeps its starting value.
            List<Integer> act = new ArrayList<>();
            for (int v = 0; v < 3; v++)
            {
                for (int b = 0; b < nBoats; b++)
                {
                    if (p.boatVariantEntries[v][b].isEmpty())
                        continue;
                    double sumW = 0;
                    for (int idx : p.boatVariantEntries[v][b])
                        sumW += baseWeights[idx];
                    if (sumW > 0 || (!Double.isNaN(rfLog[v][b]) && rfW[v][b] > 0))
                        act.add(v * nBoats + b);
                }
            }
            active = act.stream().mapToInt(Integer::intValue).toArray();
            perMove = active.length == 0 ? 0
                : Math.max(1, Math.min(active.length, (int)Math.round(annealConfig.subsetFraction() * active.length)));

            offset = new double[p.nDivs];
            s0 = new double[p.nDivs];
            s1 = new double[p.nDivs];
            s2 = new double[p.nDivs];
            moved = new int[perMove];
            movedOld = new double[perMove];
            touched = new int[p.nDivs];
            touchStamp = new int[p.nDivs];
            savedS1 = new double[p.nDivs];
            savedS2 = new double[p.nDivs];
            refreshWeights();
        }

        /** Recomputes the entry weights from the current residuals and rebuilds the division sums. */
        void refreshWeights()
        {
            w = PfScore.entryWeights(p, logPf, config, baseWeights);
            Arrays.fill(s0, 0);
            Arrays.fill(s1, 0);
            Arrays.fill(s2, 0);
            double[] sumWX = new double[p.nDivs];
            for (int i = 0; i < w.length; i++)
            {
                s0[divOrd[i]] += w[i];
                sumWX[divOrd[i]] += w[i] * corrected(i);
            }
            for (int d = 0; d < p.nDivs; d++)
                offset[d] = s0[d] > 0 ? sumWX[d] / s0[d] : 0;
            for (int i = 0; i < w.length; i++)
            {
                double x = corrected(i) - offset[divOrd[i]];
                s1[divOrd[i]] += w[i] * x;
                s2[divOrd[i]] += w[i] * x * x;
            }
        }

        private double corrected(int idx)
        {
            PfProblem.Entry e = p.entries.get(idx);
            return logElapsed[idx] + logPf[e.variant()][e.boatOrdinal()];
        }

        private static double divEnergy(double s0, double s1, double s2)
        {
            return s0 > 0 ? s2 - s1 * s1 / s0 : 0;
        }

        /**
         * Perturbs {@code count} (≤ {@link #perMove}) distinct random boat-variants by N(0, σ²)
         * in log PF and returns the energy change. The move stays applied until {@link #revert}.
         */
        double move(double sigma, int count)
        {
            int nBoats = p.nBoats;
            stamp++;
            nTouched = 0;
            nMoved = count;
            double dRf = 0;
            for (int j = 0; j < count; j++)
            {
                // Partial Fisher-Yates: the first j slots hold this move's picks
                int r = j + rng.nextInt(active.length - j);
                int code = active[r];
                active[r] = active[j];
                active[j] = code;

                int v = code / nBoats;
                int b = code % nBoats;
                double old = logPf[v][b];
                double delta = sigma * rng.nextGaussian();
                double nw = old + delta;
                moved[j] = code;
                movedOld[j] = old;

                if (!Double.isNaN(rfLog[v][b]))
                {
                    double dn = nw - rfLog[v][b];
                    double dold = old - rfLog[v][b];
                    dRf += rfW[v][b] * (dn * dn - dold * dold);
                }

                for (int idx : bvAll[v][b])
                {
                    int d = divOrd[idx];
                    if (touchStamp[d] != stamp)
                    {
                        touchStamp[d] = stamp;
                        touched[nTouched++] = d;
                        savedS1[d] = s1[d];
                        savedS2[d] = s2[d];
                    }
                    double wi = w[idx];
                    if (wi == 0)
                        continue;
                    double xo = logElapsed[idx] + old - offset[d];
                    double xn = xo + delta;
                    s1[d] += wi * delta;
                    s2[d] += wi * (xn * xn - xo * xo);
                }
                logPf[v][b] = nw;
            }

            double dVar = 0;
            for (int t = 0; t < nTouched; t++)
            {
                int d = touched[t];
                dVar += divEnergy(s0[d], s1[d], s2[d]) - divEnergy(s0[d], savedS1[d], savedS2[d]);
            }
            return dVar + config.lambda() * dRf;
        }

        /** Undoes the last {@link #move}. */
        void revert()
        {
            int nBoats = p.nBoats;
            for (int j = nMoved - 1; j >= 0; j--)
                logPf[moved[j] / nBoats][moved[j] % nBoats] = movedOld[j];
            for (int t = 0; t < nTouched; t++)
            {
                int d = touched[t];
                s1[d] = savedS1[d];
                s2[d] = savedS2[d];
            }
        }
    }
}
