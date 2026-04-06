# Error Bars for Weighted Factors

## Motivation

The project uses weights throughout to express the trustworthiness of factors: reference
factors derived from ORC certificates have weight 1.0; those propagated through many hops
of race co-participation have weight 0.04 or lower. A weight is meaningful to the
development team but opaque to an end user. Converting weights to +/- error bars gives
users an intuitive sense of how much to trust a displayed factor value.

---

## Mathematical Basis

Weights are treated as inversely proportional to variance in log space:

```
w = sigma_0^2 / sigma^2
```

Rearranging:

```
sigma(w) = sigma_0 / sqrt(w)
```

where `sigma_0` is the standard deviation at weight = 1.0, calibrated from data (see below).
All factors are handled in log space because the ALS optimisation operates in log space and
because multiplicative errors are symmetric in log space (being 10% fast and 10% slow are
equidistant from the true value).

A 95% confidence interval for a factor value `f` with weight `w` is:

```
[ exp(log(f) - 2 * sigma(w)),  exp(log(f) + 2 * sigma(w)) ]
```

For small sigma this is approximately `f +/- 2 * sigma(w) * f` in linear space.

---

## Calibrating sigma_0

`sigma_0` is calibrated empirically in two stages. The first stage uses ORC one-design
certificates and is independent of the ALS optimisation. The second stage cross-checks
against ALS residuals once the optimiser is running.

### Stage 1: ORC One-Design Certificates (primary calibration)

Within a one-design class, all boats should have the same theoretical TCF. The observed
spread of TCF values across certificated boats of the same class is therefore pure
measurement noise plus minor boat-to-boat variation -- the irreducible uncertainty at
weight = 1.0.

**Procedure:**

1. From the ingested ORC certificate data, select all certificates where `IsOd` is true
   (one-design flag).

2. Group by normalised design name (using the ORC `Class` field as the canonical name).

3. For each class with at least 5 certificated boats, convert GPH to TCF:
   ```
   TCF = 600 / GPH
   ```
   and compute the standard deviation in log space:
   ```
   s_class = std( log(TCF_i) )   for all boats i in this class
   ```

4. Take the median of `s_class` across all qualifying classes. This is `sigma_0`.

Expected result: `sigma_0` in the range 0.01 to 0.03 (1% to 3%). A value outside this
range warrants investigation before proceeding.

**Why median rather than mean:** a single class with unusual inter-boat variation (e.g. an
open one-design where hull modifications are permitted) would distort the mean; the median
is robust to such outliers.

### Stage 2: ALS Residual Cross-Check (validation once optimiser is running)

Once the ALS optimisation has converged, boats that hold ORC certificates and have raced
enough to have a well-converged HPF provide a second estimate of `sigma_0`. The difference
between their optimised HPF and their certificate-derived TCF should be small and centred
near zero.

**Procedure:**

1. Select all boats where:
   - ORC certificate weight >= 0.9 (certificate held and used in the race)
   - At least 10 races in the optimisation scope
   - HPF confidence is above a threshold (to be defined when the optimiser is running)

2. Compute:
   ```
   delta_i = log(HPF_i) - log(TCF_from_certificate_i)
   ```

3. Compute `std(delta_i)` across all qualifying boats. This is the ALS-derived estimate
   of `sigma_0`.

**Interpretation:**

- If the ALS estimate agrees with the ORC one-design estimate within a factor of 2, use
  the ORC-derived value. It is independent of the optimisation and therefore a cleaner
  anchor.

- If the ALS estimate is substantially larger (say, more than twice the ORC estimate),
  real-world racing variability dominates pure measurement uncertainty. In this case use
  the ALS-derived value, as it better reflects what a user would actually observe if they
  checked a boat's HPF against its certificate.

- Record both estimates in the application configuration so the difference is visible and
  the choice is documented, not hidden.

---

## Implementation

### Named Constant

`sigma_0` must be a named constant, not a magic number:

```java
/** Reference standard deviation in log space at weight = 1.0.
 *  Calibrated from ORC one-design certificate spread; see error_bars.md. */
public static final double SIGMA_0 = 0.020;  // update after calibration
```

The value 0.020 is a reasonable prior before calibration is run. Replace it with the
empirically derived value from the Stage 1 procedure above.

### sigma(w) Computation

```java
/** Standard deviation in log space for a factor with the given weight. */
public static double sigma(double weight) {
    if (weight <= 0.0) throw new IllegalArgumentException("weight must be positive");
    return SIGMA_0 / Math.sqrt(weight);
}
```

### 95% Confidence Interval in Linear Space

```java
public record FactorWithBars(double factor, double lower95, double upper95) {}

public static FactorWithBars confidenceInterval(double factor, double weight) {
    double s = sigma(weight);
    return new FactorWithBars(
        factor,
        Math.exp(Math.log(factor) - 2 * s),
        Math.exp(Math.log(factor) + 2 * s)
    );
}
```

### Display Cap

At very low weights the interval expands without bound. Cap displayed error bars at
+/- 3 * SIGMA_0 * factor in linear space (approximately +/- 6% if SIGMA_0 = 0.02) to
avoid bars wider than the factor value itself being shown in the UI. Values hitting the
cap should be displayed with a visual indicator (e.g. a dashed rather than solid error
bar) to signal that the true uncertainty is larger than shown.

Zero-weight values must not be displayed with error bars at all -- they are in the skip
category, not the low-confidence category.

---

## Illustrative Values

Using `sigma_0 = 0.020` (to be replaced with calibrated value):

| Weight | sigma  | +/- 95% (approx linear) |
|--------|--------|------------------------|
| 1.00   | 0.020  | +/- 4.0%               |
| 0.90   | 0.021  | +/- 4.2%               |
| 0.80   | 0.022  | +/- 4.5%               |
| 0.70   | 0.024  | +/- 4.8%               |
| 0.40   | 0.032  | +/- 6.3%               |
| 0.10   | 0.063  | +/- 12.6%              |
| 0.04   | 0.100  | +/- 20.0%              |

A boat with factor 0.90 and weight 0.85 would display as approximately 0.90 +/- 0.038,
or equivalently [0.862, 0.938] at 95% confidence.

---

## Relationship to HPF Confidence

The weight-derived error bars described here apply to **reference factors** -- the anchors
fed into the ALS optimisation. The HPF output from the optimisation has its own confidence
measure derived from per-race residuals (Step 19 of the processing pipeline):

```
sigma_HPF = sqrt( sum(w_j * r_j^2) / sum(w_j) )
```

where `r_j = log(e_ij) - log(h_i) - log(t_j)` is the residual for race j.

These two quantities are distinct and both should be displayed where relevant:

- **Reference factor +/- bars** -- how well we know the boat's theoretical speed from
  certificates and propagation. Shown on the reference factor display.
- **HPF residual sigma** -- how consistently the boat actually races relative to its HPF.
  Shown on the HPF display. A wide residual sigma means the boat is variable race to race,
  regardless of how well-anchored its reference factor is.
