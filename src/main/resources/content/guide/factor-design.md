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
HPF estimated from 8 different races. This is not a simple average: the combined weight
must reflect both the mean quality of the inputs and the consistency among them.

#### Combined value

Weighted mean of the input values:

```
combinedValue = Σ(wᵢ × vᵢ) / Σwᵢ
```

#### Combined weight

Weighted mean input weight, penalised by the spread of values:

```
meanInputWeight  = Σwᵢ / n

weightedVariance = Σ(wᵢ × (vᵢ − combinedValue)²) / Σwᵢ

combinedWeight   = meanInputWeight / (1 + weightedVariance / σ₀²)
```

Where `σ₀` is a tunable scale parameter representing "how much variance halves
confidence?" Suggested starting value: **`σ₀ = 0.05`** (a standard deviation of 5% in
factor value space halves the combined weight).

#### Behaviour

| Input spread | Effect on `combinedWeight` |
|---|---|
| All inputs identical → `weightedVariance = 0` | `combinedWeight = meanInputWeight` (no penalty) |
| Inputs moderately scattered | `combinedWeight < meanInputWeight` |
| Inputs highly scattered | `combinedWeight` substantially reduced |

The "more samples → higher weight" effect is implicit: more races feeding `aggregate`
tends to produce a lower `weightedVariance` and a higher `meanInputWeight`, both of which
push `combinedWeight` upward. No separate sample-count multiplier is needed.

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

- `aggregate` requires the tuning constant `σ₀`. This must be a named constant or
  configuration parameter — not a magic number buried in the implementation.

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
