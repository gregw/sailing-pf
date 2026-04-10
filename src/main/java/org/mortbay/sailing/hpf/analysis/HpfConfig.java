package org.mortbay.sailing.hpf.analysis;

/**
 * Immutable parameters for the HPF optimiser.
 */
public record HpfConfig(
    double lambda,                  // regularisation strength (pulls HPF toward RF)
    double convergenceThreshold,    // max |Δlog(HPF)| per inner iteration for convergence
    int maxInnerIterations,         // ALS convergence limit
    int maxOuterIterations,         // reweighting cycles
    double outlierK,                // IQR multiplier for entry down-weighting (Cauchy scale)
    double asymmetryFactor,         // extra penalty for fast outliers (residual < 0)
    double outerDampingFactor,      // blend fraction for outer weight updates (1.0=no damping, 0.5=half step)
    double outerConvergenceThreshold, // max weight change to declare outer convergence (default 0.01)
    double crossVariantLambda       // couples variant HPFs via RF ratio; 0 = disabled
)
{
    public static final HpfConfig DEFAULT = new HpfConfig(1.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0);
}
