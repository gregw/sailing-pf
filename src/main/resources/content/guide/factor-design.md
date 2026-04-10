# `Factor` and `WeightedInterval` — Design and Algorithms

## Core Types

```java
record Factor(double value, double weight) {
    // value:  dimensionless time correction ratio, ~1.0000, range (0, ~2.0]
    // weight: scalar in [0, 1]; 0 = discard entirely
}

record WeightedInterval(Duration duration, double weight) {
    // duration: elapsed time
    // weight:   scalar in [0, 1]
}
```

`Factor` and `WeightedInterval` are structurally parallel: both carry a `weight` in [0, 1]
by the same convention. `Factor` holds a dimensionless ratio; `WeightedInterval` holds a
`Duration`. Weight of zero means "discard" — callers and implementations should skip
zero-weight inputs rather than letting them silently distort results.

---

## Operations

### `apply(Factor f, WeightedInterval t) → WeightedInterval`

Applies a handicap factor to a weighted elapsed time to produce a corrected time.

```
result.duration = t.duration × f.value
result.weight   = t.weight × f.weight
```

Two uncertain things composed together are more uncertain than either alone, so weights
multiply. This is the correct composition for dependent uncertainties.

---

### `compose(Factor a, Factor b) → Factor`

Chains two **independent multiplicative adjustments** — for example, a certificate value
composed with a propagation step through a race network hop. Used when the two factors
represent different stages of a causal chain, not two estimates of the same quantity.

```
result.value  = a.value × b.value
result.weight = a.weight × b.weight
```

`compose` is commutative and associative with respect to value arithmetic. Weights multiply
because independent uncertainties compound.

---

### `aggregate(Factor[] factors) → Factor`

Combines multiple **independent estimates of the same quantity** — for example, a boat's
HPF estimated from 8 different races, or certificate-based factors from multiple
conversion paths. This is the primary combiner: it accumulates evidence (more agreeing
observations increase confidence) while penalising disagreement.

#### Combined value

Weighted mean of the input values:

```
combinedValue = Σ(wᵢ × vᵢ) / Σwᵢ
```

#### Combined weight

Pooled evidence, penalised by the spread of values:

```
pooledWeight     = 1 − ∏(1 − wᵢ)

weightedVariance = Σ(wᵢ × (vᵢ − combinedValue)²) / Σwᵢ

combinedWeight   = pooledWeight / (1 + weightedVariance / σ₀²)
```

The `pooledWeight` formula (`1 − ∏(1 − wᵢ)`) is the "at least one good measurement"
accumulator — it always increases with more inputs, is bounded [0, 1], and adding a
zero-weight input is a no-op. This ensures that repeated, agreeing evidence always
increases confidence.

`σ₀` is a tunable scale parameter representing "how much variance halves confidence?"
Value: **`σ₀ = 0.15`** (a standard deviation of 15% in factor value space halves the
combined weight).

#### Behaviour

| Input spread | Effect on `combinedWeight` |
|---|---|
| All inputs identical → `weightedVariance = 0` | `combinedWeight = pooledWeight` (no penalty, always ≥ max input weight) |
| Inputs close together | `combinedWeight` increases above max input weight, but less than identical case |
| Inputs moderately scattered (large weights) | `combinedWeight` reduced below individual input weights |
| Inputs moderately scattered (small weights) | Evidence accumulation still wins; weight increases above inputs |
| Inputs highly scattered | `combinedWeight` substantially reduced |

---

## Summary Table

| Operation | Inputs | Output | Purpose |
|---|---|---|---|
| `apply` | `Factor`, `WeightedInterval` | `WeightedInterval` | Apply a handicap to an elapsed time |
| `compose` | `Factor`, `Factor` | `Factor` | Chain independent multiplicative steps |
| `aggregate` | `Factor[]` | `Factor` | Combine multiple estimates of the same value |

---

## Implementation Notes

- These are pure value-type operations — no side effects, no dependencies on any other
  layer. They belong on the `Factor` record as static methods, or in a companion
  `Factors` utility class.

- `aggregate` requires the tuning constant `σ₀` (currently 0.15). This must be a named
  constant or configuration parameter — not a magic number buried in the implementation.

- `WeightedInterval` is structurally parallel to `Factor` but holds a `Duration` instead
  of a dimensionless ratio. Both carry `weight` in [0, 1] by the same convention.

- Weight of zero means "discard". Implementations must skip zero-weight factors rather
  than allowing them to silently distort results.

- `Factor` has a nice closure property: the output of `aggregate` is itself a `Factor`,
  so aggregation can be applied at multiple levels (race → series → fleet) without
  changing the type.

- The expected value range for `Factor.value` is (0, ~2.0], open-ended on the upper side.
  Slower boats have values above 1.0; faster boats below 1.0. The value 1.000 represents
  the hypothetical reference boat against which all HPF calculations are anchored.
