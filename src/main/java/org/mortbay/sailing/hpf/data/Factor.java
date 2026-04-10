package org.mortbay.sailing.hpf.data;

import java.time.Duration;

/**
 * A dimensionless time correction ratio with an associated confidence weight.
 * <p>
 * {@code value}  — time correction factor, range (0, ~2.0]. Slower boats have values
 * above 1.0; faster boats below 1.0. The value 1.000 represents the hypothetical
 * reference boat against which all HPF calculations are anchored.
 * <br>
 * {@code weight} — scalar in [0, 1]. Weight of zero means "discard" — callers and
 * implementations must skip zero-weight factors rather than letting them silently
 * distort results.
 * <p>
 * Three core operations are provided as static methods:
 * <ul>
 *   <li>{@link #apply}     — apply a factor to an elapsed time</li>
 *   <li>{@link #compose}   — chain two independent multiplicative factors</li>
 *   <li>{@link #aggregate} — combine multiple estimates of the same quantity</li>
 * </ul>
 */
public record Factor(
    double value,   // dimensionless time correction ratio, ~1.0, range (0, ~2.0]
    double weight   // scalar in [0, 1]; 0 = discard
)
{
    /**
     * Scale parameter for {@link #aggregate}: a standard deviation of this magnitude
     * in factor-value space halves the combined weight. Suggested starting value: 0.05
     * (5% spread in factor values halves confidence).
     */
    public static final double SIGMA_0 = 0.05;

    public Factor
    {
        if (value <= 0)
            throw new IllegalArgumentException("Factor value must be positive, got: " + value);
        if (weight < 0 || weight > 1)
            throw new IllegalArgumentException("Factor weight must be in [0, 1], got: " + weight);
    }

    // --- The three prime operations ---

    /**
     * Applies a handicap factor to a weighted elapsed time, producing a corrected time.
     * <pre>
     *   result.duration = t.duration × f.value
     *   result.weight   = t.weight × f.weight
     * </pre>
     * Weights multiply because two uncertain quantities composed together are more
     * uncertain than either alone.
     */
    public static WeightedInterval apply(Factor f, WeightedInterval t)
    {
        Duration corrected = Duration.ofNanos((long)(t.duration().toNanos() * f.value()));
        return new WeightedInterval(corrected, t.weight() * f.weight());
    }

    /**
     * Applies a handicap factor to an unweighted elapsed time, producing a corrected time.
     * The result carries the factor's own weight, since the duration contributes no
     * additional uncertainty.
     * <pre>
     *   result.duration = t × f.value
     *   result.weight   = f.weight
     * </pre>
     */
    public static WeightedInterval apply(Factor f, Duration t)
    {
        Duration corrected = Duration.ofNanos((long)(t.toNanos() * f.value()));
        return new WeightedInterval(corrected, f.weight());
    }

    /**
     * Chains one or more independent multiplicative adjustments into a single factor.
     * <pre>
     *   result.value  = f₁.value × f₂.value × …
     *   result.weight = f₁.weight × f₂.weight × …
     * </pre>
     * Use this when the inputs represent different stages of a causal chain
     * (e.g. a certificate value composed with a propagation hop), not multiple
     * estimates of the same quantity. For the latter, use {@link #aggregate}.
     * <p>
     * {@code compose} is commutative and associative on values. Weights multiply
     * because independent uncertainties compound.
     *
     * @param factors one or more factors to chain; must not be empty
     * @throws IllegalArgumentException if no factors are provided
     */
    public static Factor compose(Factor... factors)
    {
        if (factors.length == 0)
            throw new IllegalArgumentException("compose requires at least one factor");
        double value = 1.0, weight = 1.0;
        for (Factor f : factors)
        {
            value  *= f.value();
            weight *= f.weight();
        }
        return new Factor(value, weight);
    }

    /**
     * Blends two or more factors where adding more evidence should never reduce confidence.
     * This is the right combiner when the inputs are estimates from different sources
     * (e.g. cert paths from different systems, or a boat RF vs its design-class RF) and
     * disagreement between sources is expected and structural, not a sign of uncertainty.
     *
     * <p>Compare with {@link #aggregate}: {@code aggregate} uses {@code meanInputWeight / n}
     * so adding a second, lower-weight path always reduces the combined weight — wrong here.
     * {@code combine} uses {@code max(wᵢ)} so additional evidence never hurts.
     *
     * <p>Value: log-space weighted mean (appropriate for multiplicative TCF ratios).
     * <pre>
     *   combinedValue = exp( Σ(wᵢ × log(vᵢ)) / Σwᵢ )
     * </pre>
     * Weight: maximum of all input weights (more evidence ≥ best single evidence).
     * <pre>
     *   combinedWeight = max(wᵢ)
     * </pre>
     * Zero-weight inputs are skipped; returns {@code null} if all inputs are null or zero-weight.
     *
     * <p><b>Typical uses:</b>
     * <ul>
     *   <li>Step 8 — combining cert-path factors from IRC, ORC, and AMS into one RF estimate</li>
     *   <li>Step 13 — blending a boat's cert-based RF with the design-class RF as a prior</li>
     * </ul>
     */
    public static Factor combine(Factor... factors)
    {
        double sumW = 0, sumWLogV = 0, maxW = 0;
        int n = 0;
        for (Factor f : factors)
        {
            if (f != null && f.weight() > 0)
            {
                sumW     += f.weight();
                sumWLogV += f.weight() * Math.log(f.value());
                if (f.weight() > maxW) maxW = f.weight();
                n++;
            }
        }
        if (n == 0) return null;
        return new Factor(Math.exp(sumWLogV / sumW), maxW);
    }

    /**
     * Combines multiple independent estimates of the same quantity into a single factor.
     * <p>
     * Combined value: weighted mean of input values.
     * <pre>
     *   combinedValue = Σ(wᵢ × vᵢ) / Σwᵢ
     * </pre>
     * Combined weight: mean input weight, penalised by the spread of values.
     * <pre>
     *   meanInputWeight  = Σwᵢ / n
     *   weightedVariance = Σ(wᵢ × (vᵢ − combinedValue)²) / Σwᵢ
     *   combinedWeight   = meanInputWeight / (1 + weightedVariance / σ₀²)
     * </pre>
     * Where {@code σ₀} is {@link #SIGMA_0}. Zero-weight inputs are skipped.
     *
     * <p>Use this when inputs are N independent noisy observations of the <em>same</em> quantity
     * and disagreement between them reflects genuine uncertainty (e.g. cross-race HPF estimates
     * for one boat). When inputs come from different sources and structural disagreement is
     * expected, use {@link #combine} instead.
     *
     * @param factors one or more factor estimates; zero-weight entries are ignored
     * @return the aggregated factor
     * @throws IllegalArgumentException if no non-zero-weight inputs exist
     */
    public static Factor aggregate(Factor... factors)
    {
        // Filter zero-weight inputs
        double sumW = 0, sumWV = 0;
        int n = 0;
        for (Factor f : factors)
        {
            if (f.weight() > 0)
            {
                sumW  += f.weight();
                sumWV += f.weight() * f.value();
                n++;
            }
        }

        if (n == 0)
            throw new IllegalArgumentException("aggregate requires at least one non-zero-weight input");

        double combinedValue = sumWV / sumW;

        double meanInputWeight = sumW / n;

        double sumWVar = 0;
        for (Factor f : factors)
        {
            if (f.weight() > 0)
            {
                double diff = f.value() - combinedValue;
                sumWVar += f.weight() * diff * diff;
            }
        }
        double weightedVariance = sumWVar / sumW;

        double combinedWeight = meanInputWeight / (1.0 + weightedVariance / (SIGMA_0 * SIGMA_0));

        return new Factor(combinedValue, combinedWeight);
    }
}
