package org.mortbay.sailing.hpf.analysis;

/**
 * Result of an ordinary least-squares fit: y = slope·x + intercept.
 * All values are in TCF (time correction factor) space.
 * <p>
 * {@link #predict} gives the expected y for a query x.
 * {@link #weight} gives a confidence score in [0, 1] for that prediction,
 * combining global fit quality (R²) with local precision (prediction interval width).
 * A weight of 0 means "discard this conversion"; 1 means "full confidence".
 */
public record LinearFit(
    double slope,
    double intercept,
    double r2,       // coefficient of determination ∈ [0, 1]
    double se,       // residual standard error = sqrt(SSres / (n-2))
    int n,
    double xMean,
    double ssx       // Σ(xᵢ - x̄)²
)
{
    /**
     * Reference scale for the weight function: a prediction standard error of this
     * magnitude in TCF space halves the weight contribution from precision alone.
     * 0.02 = 2% TCF — reflects acceptable uncertainty for initial handicap allocation.
     */
    public static final double SIGMA_REF = 0.02;

    /**
     * Predicted y value at query point x₀.
     */
    public double predict(double x)
    {
        return slope * x + intercept;
    }

    /**
     * Computes the inverse fit: if this fit models y = slope·x + intercept (e.g. spin from NS),
     * returns a new LinearFit modelling x from y (NS from spin).
     * <p>
     * Prediction uses the simple algebraic inverse: x = (y − intercept) / slope.
     * The weight statistics (se, xMean, ssx) are derived from OLS theory so that
     * {@link #weight(double)} returns a sensible confidence value for the inverse prediction.
     */
    public LinearFit inverse()
    {
        double slopeInv     = 1.0 / slope;
        double interceptInv = -intercept / slope;
        double seInv        = se * Math.sqrt(r2) / Math.abs(slope);  // se_inv = se * √R² / |slope|
        double xMeanInv     = slope * xMean + intercept;              // mean of original y values
        double ssxInv       = slope * slope * ssx / r2;               // SSyy of forward fit
        return new LinearFit(slopeInv, interceptInv, r2, seInv, n, xMeanInv, ssxInv);
    }

    /**
     * Confidence weight for the prediction at x₀, combining:
     * <ul>
     *   <li>Global: R² — low if the regression line is a poor overall fit</li>
     *   <li>Local: prediction interval width — low if x₀ is far from the data centroid
     *       or if the residual scatter is large</li>
     * </ul>
     * Formula:
     * <pre>
     *   sePred = se · √(1 + 1/n + (x₀ − x̄)² / SSxx)
     *   weight = R² / (1 + (sePred / σ₀)²)
     * </pre>
     */
    public double weight(double x)
    {
        double sePred = se * Math.sqrt(1.0 + 1.0 / n + (x - xMean) * (x - xMean) / ssx);
        return r2 / (1.0 + (sePred / SIGMA_REF) * (sePred / SIGMA_REF));
    }
}
