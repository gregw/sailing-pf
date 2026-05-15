package org.mortbay.sailing.pf.analysis;

/**
 * Immutable parameters for the PF optimiser.
 *
 * <p>{@code crossVariantLambda} and {@code graphCrossVariantLambda} both couple a boat's
 * spin/nonSpin/twoHanded PFs to one another, but via different anchors:
 * <ul>
 *   <li>{@code crossVariantLambda} pulls toward the BOAT's own RF-implied ratio between
 *       variants — preserves whatever spin/nonSpin offset the boat's certificates imply.
 *       Defaults to 0 (disabled).</li>
 *   <li>{@code graphCrossVariantLambda} pulls toward the FLEET-WIDE conversion-graph ratio
 *       for the target year — predicts each variant from the others using the linear fit
 *       between IRC variant nodes. Defaults to 0.25 (mild always-on prior).</li>
 * </ul>
 * Both terms can be enabled simultaneously; their effects sum.
 *
 * <p>{@code noRaceFallbackWeight} caps the confidence assigned to a boat's PF for a variant
 * in which the boat has zero race entries AND from which no cross-variant graph inference
 * is available (e.g. graph not loaded, or no other variant has races). The PF value falls
 * back to the boat's RF, but its weight is capped at this value to reflect that we have no
 * direct observation of the boat in this variant. Defaults to 0.2.
 */
public record PfConfig(
    double lambda,                  // regularisation strength (pulls PF toward RF)
    double convergenceThreshold,    // max |Δlog(PF)| per inner iteration for convergence
    int maxInnerIterations,         // ALS convergence limit
    int maxOuterIterations,         // reweighting cycles
    double outlierK,                // IQR multiplier for entry down-weighting (Cauchy scale)
    double asymmetryFactor,         // extra penalty for fast outliers (residual < 0)
    double outerDampingFactor,      // blend fraction for outer weight updates (1.0=no damping, 0.5=half step)
    double outerConvergenceThreshold, // max weight change to declare outer convergence (default 0.01)
    double crossVariantLambda,      // couples variant PFs via THIS BOAT's RF ratio; 0 = disabled
    double graphCrossVariantLambda, // couples variant PFs via FLEET-WIDE conversion graph; 0 = disabled
    double noRaceFallbackWeight,    // cap on PF weight for a variant with no races and no usable graph inference
    double outerPfConvergenceThreshold, // max |Δlog(PF)| between outer cycles; outer loop also converges when this is met
    boolean logOuterDiagnostics,    // when true, log top weight-flippers and PF-movers each outer cycle
    double dubiousFactor,           // PF threshold above which entry weights ramp down (default 1.5)
    double maxFactor                // PF threshold at/above which entry weights are zero (default 2.0)
)
{
    public static final PfConfig DEFAULT = new PfConfig(
        1.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0, 0.25, 0.2, 1.0e-3, false, 1.5, 2.0);
}
