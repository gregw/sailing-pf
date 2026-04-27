package org.mortbay.sailing.pf.analysis;

/**
 * Five-spoke performance profile for one boat, all scores in [0, 1].
 * Scores are fleet-relative percentile ranks computed across all active boats
 * using the last 12 months of data. A score of 1.0 means best in fleet on that spoke;
 * 0.0 means worst.
 * <p>
 * Computed by {@link PerformanceProfileBuilder#buildAll} after each PF run.
 */
public record PerformanceProfile(
    double frequency,     // percentile rank by duration-weighted race count × small year-spread bonus
    double diversity,     // percentile rank by variant-weighted Σ√(encounters) per (opponent, variant)
    double consistency,   // percentile rank by mean asymmetric r² (negative residuals weighted more); lower = better
    double stability,     // percentile rank by asymmetric slope penalty (level=best, declining=worst)
    double chaotic,       // percentile rank by mean r² weighted by 1/dispersion; lower = better
    double overallScore   // normalised polygon area [0,1]
) {}
