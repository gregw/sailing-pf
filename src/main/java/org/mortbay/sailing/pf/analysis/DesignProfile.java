package org.mortbay.sailing.pf.analysis;

/**
 * Six-spoke design profile for one design, all scores in [0, 1].
 * Scores are percentile ranks across all designs with at least
 * {@value DesignProfileBuilder#MIN_ACTIVE_BOATS} active boats, using the last 12 months of data.
 * 1.0 means best among designs on that spoke; 0.0 means worst.
 * <p>
 * Computed by {@link DesignProfileBuilder#buildAll} after each PF run.
 */
public record DesignProfile(
    double quantity,      // percentile rank by active boats of the design
    double ubiquity,      // percentile rank by effective number of clubs (inverse Simpson of boats per club)
    double frequency,     // percentile rank by mean distinct races per active boat
    double results,       // percentile rank by the boats' mean PF-corrected placing percentile
    double consistency,   // percentile rank by RMS of log(PF / design RF) across active boats; lower = better
    double certified,     // percentile rank by share of active boats with a recent measurement certificate
    double overallScore,  // normalised polygon area [0,1]
    int activeBoats       // the design's active boats (raw Quantity)
) {}
