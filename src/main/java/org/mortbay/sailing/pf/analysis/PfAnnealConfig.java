package org.mortbay.sailing.pf.analysis;

/**
 * Parameters for {@link PfAnnealer}. The objective's own parameters (λ, outlier K, asymmetry,
 * age half-life, dubious/max factor) come from {@link PfConfig}, shared with the ALS optimiser.
 */
public record PfAnnealConfig(
    double subsetFraction,      // fraction of boat-variants perturbed per move (0 < f ≤ 1); at least one
    double stepSize,            // std dev of a single-PF step in log PF (0.02 ≈ 2%); divided by √(PFs per move), then adapted
    double coolingRate,         // temperature multiplier per stage (0 < r < 1)
    int movesPerTemperature,    // moves tried at each temperature
    int maxStages,              // upper limit on the number of temperatures
    double minTemperatureRatio, // stop once T falls below this fraction of the starting temperature
    long seed                   // random seed — the same seed and data give the same result
)
{
    public static final PfAnnealConfig DEFAULT = new PfAnnealConfig(0.02, 0.02, 0.95, 2000, 300, 1.0e-6, 1);
}
