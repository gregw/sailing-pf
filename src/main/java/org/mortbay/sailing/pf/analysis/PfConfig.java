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
    double graphCrossVariantLambda  // couples variant PFs via FLEET-WIDE conversion graph; 0 = disabled
)
{
    public static final PfConfig DEFAULT = new PfConfig(1.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0, 0.25);
}
