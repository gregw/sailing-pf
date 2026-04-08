package org.mortbay.sailing.hpf.analysis;

/**
 * Five-spoke performance profile for one boat, all scores in [0, 1].
 * Scores are fleet-relative percentile ranks computed across all active boats
 * using the last 12 months of data. A score of 1.0 means best in fleet on that spoke;
 * 0.0 means worst.
 * <p>
 * Computed by {@link PerformanceProfileBuilder#buildAll} after each HPF run.
 */
public record PerformanceProfile(
    double frequency,     // percentile rank by race count last 12m
    double diversity,     // percentile rank by distinct opponents last 12m
    double consistency,   // percentile rank by sum-of-squared residuals last 12m (lower = better)
    double stability,     // percentile rank by asymmetric slope penalty (level=best, declining=worst)
    double nonChaotic,    // percentile rank by correlation of |residual| with race dispersion
    double overallScore   // normalised polygon area [0,1]
) {}
