package org.mortbay.sailing.pf.analysis;

import java.util.Arrays;
import java.util.List;

/**
 * How well a set of PFs fits the races: the weighted variance of corrected times within each
 * division, plus a penalty for straying from each boat's reference factor. The one figure used
 * to compare optimisers — {@link PfOptimiser} (ALS) and {@link PfAnnealer} (simulated annealing)
 * are scored by the same {@link #of} on the same {@link PfProblem}, and lower is better.
 * <p>
 * Corrected times are in log space ({@code x = log(elapsed) + log(PF)}), so a division's
 * variance is a relative spread: a long race counts the same as a short one, and scaling every
 * PF together changes nothing — the RF term is what anchors the level. Each division adds
 * {@code Σ wᵢ (xᵢ − x̄)²} about its weighted mean, so bigger divisions count for more. An entry's
 * weight is its RF confidence × age decay × outlier down-weight × fast-boat ramp, the outlier
 * weight coming from the solution being scored (its residual against the division's weighted
 * median, scaled by the division's IQR), so the score is a deterministic function of the PFs.
 */
public record PfScore(
    double energy,              // varianceTerm + rfTerm — what PfAnnealer minimises
    double varianceTerm,        // Σ_d Σ_i wᵢ (xᵢ − x̄_d)²
    double rfTerm,              // λ Σ rfW (log PF − log RF)²
    double totalWeight,         // Σ wᵢ over all entries
    double weightedVariance,    // varianceTerm / totalWeight — comparable between data sets
    double rmsSpread,           // √weightedVariance: typical corrected-time spread as a fraction (0.03 ≈ 3%)
    double medianAbsResidual,   // median |xᵢ − x̄_d|
    double pct95AbsResidual,    // 95th percentile |xᵢ − x̄_d|
    int entries,
    int divisions
)
{
    /** RF confidence × age decay of each entry: its weight before outlier and fast-boat down-weighting. */
    static double[] baseWeights(PfProblem p)
    {
        double[] w = new double[p.entries.size()];
        for (int i = 0; i < w.length; i++)
            w[i] = p.entries.get(i).refWeight() * p.ageDecay[i];
        return w;
    }

    /**
     * Full entry weights for {@code logPf}: base weight × outlier weight × fast-boat ramp.
     * The outlier weight measures each residual against its division's weighted median, in
     * units of {@code outlierK} × the division's IQR (as {@link PfOptimiser} does).
     */
    static double[] entryWeights(PfProblem p, double[][] logPf, PfConfig config, double[] baseWeights)
    {
        double[] logT = new double[p.nDivs];
        double[] iqr = new double[p.nDivs];
        PfOptimiser.computeT0(p.entries, p.divEntryIndexes, logPf, baseWeights, logT, iqr);
        double[] w = new double[baseWeights.length];
        for (int i = 0; i < w.length; i++)
        {
            PfProblem.Entry e = p.entries.get(i);
            w[i] = baseWeights[i]
                * PfOptimiser.outlierWeight(p.residual(i, logPf, logT), iqr[e.divOrdinal()], config)
                * PfOptimiser.fastBoatMultiplier(logPf[e.variant()][e.boatOrdinal()], config);
        }
        return w;
    }

    /** λ Σ rfW (log PF − log RF)² over every boat-variant with an RF. */
    static double rfTerm(PfProblem p, double[][] logPf, PfConfig config)
    {
        double sum = 0;
        for (int b = 0; b < p.nBoats; b++)
        {
            String boatId = p.ordinalToBoatId[b];
            double[] rfLog = p.boatLogRf.get(boatId);
            double[] rfW = p.boatRfWeight.get(boatId);
            for (int v = 0; v < 3; v++)
            {
                if (Double.isNaN(rfLog[v]))
                    continue;
                double d = logPf[v][b] - rfLog[v];
                sum += rfW[v] * d * d;
            }
        }
        return config.lambda() * sum;
    }

    /** Scores {@code logPf} ([variant][boat ordinal] of {@code p}). */
    static PfScore of(PfProblem p, double[][] logPf, PfConfig config)
    {
        double[] w = entryWeights(p, logPf, config, baseWeights(p));
        int n = p.entries.size();
        double[] absResiduals = new double[n];
        double varianceTerm = 0;
        double totalWeight = 0;
        for (int d = 0; d < p.nDivs; d++)
        {
            List<Integer> idxs = p.divEntryIndexes.get(d);
            double sumW = 0, sumWX = 0;
            for (int idx : idxs)
            {
                sumW += w[idx];
                sumWX += w[idx] * corrected(p, idx, logPf);
            }
            double mean = sumW > 0 ? sumWX / sumW : 0;
            for (int idx : idxs)
            {
                double r = corrected(p, idx, logPf) - mean;
                varianceTerm += w[idx] * r * r;
                absResiduals[idx] = Math.abs(r);
            }
            totalWeight += sumW;
        }
        Arrays.sort(absResiduals);
        double rfTerm = rfTerm(p, logPf, config);
        double weightedVariance = totalWeight > 0 ? varianceTerm / totalWeight : 0;
        return new PfScore(varianceTerm + rfTerm, varianceTerm, rfTerm, totalWeight,
            weightedVariance, Math.sqrt(weightedVariance),
            PfOptimiser.simpleQuantile(absResiduals, 0.5),
            PfOptimiser.simpleQuantile(absResiduals, 0.95),
            n, p.nDivs);
    }

    private static double corrected(PfProblem p, int idx, double[][] logPf)
    {
        PfProblem.Entry e = p.entries.get(idx);
        return e.logElapsed() + logPf[e.variant()][e.boatOrdinal()];
    }
}
