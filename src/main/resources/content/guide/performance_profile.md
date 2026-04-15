# Boat Performance Profile

## Overview

In addition to the HPF value itself, the HPF analysis produces enough information to
characterise *how* a boat is being sailed, not just how fast it is. This is captured
as a **Performance Profile** — a set of five measures derived from the per-race residuals
produced by the HPF optimisation, presented as a radar (pentagon) chart.

The profile is a companion to the HPF value, not a replacement for it. HPF answers
"how fast is this boat on average". The performance profile answers "how actively, reliably,
and consistently is this boat sailed relative to that average".

---

## Residuals

All profile measures are derived from the per-race log-residuals computed by
the HPF optimisation:

```
residual_i = log(elapsedTime_i × HPF_boat) − log(T_race_i)
```

A negative residual means the boat was faster than its HPF predicts in that race.
A positive residual means it was slower.

---

## Fleet-Relative Scoring

All five spokes are **fleet-relative percentile scores**: each boat is ranked within
the active fleet (boats with at least 3 distinct races in the last 12 months) and
the score is the percentile rank, mapped to [0, 1]. A score of 1.0 means best in
fleet on that spoke; 0.0 means worst.

This approach makes the chart immediately legible — an average boat in every dimension
scores 0.5 on every spoke — and means scores are self-calibrating as the fleet grows.

All measures use the **last 12 months** of race data only.

---

## The Five Spokes

### 1. Frequency

**What it measures:** how much time the boat spends racing.

**Derivation:** percentile rank of a duration-weighted race count. Each distinct race
contributes *(race duration / fleet-median duration)^α* to the score, where α ≈ 0.26
(a race twice as long counts approximately 1.2× more). This rewards boats that race
frequently or compete in longer offshore events.

**Interpretation:** a high frequency score means the boat is an active participant in
the fleet. This spoke is included because a boat with rich race data is more useful to
handicappers than an occasionally-racing boat with the same HPF. It is not a measure of
sailing quality, but it does underpin the reliability of the other spokes.

---

### 2. Consistency

**What it measures:** how tightly the boat's results cluster around its HPF.

**Derivation:** percentile rank of the mean squared residual across recent race entries
(lower MSR = better = higher percentile). Every race counts equally regardless of whether
it was down-weighted in the HPF optimiser, so a boat with occasional large residuals is
correctly penalised.

**Discards:** a configurable number of the most-divergent results are excluded before
computing the mean, similar to series scoring. One discard is allowed per *N* distinct
races sailed (default N = 11): 1 discard at 11 races, 2 at 22, and so on. This rewards
boats with long track records by forgiving occasional bad days.

**Interpretation:** a high consistency score means the boat produces similar results
race after race. This reflects stable crew, preparation, and boat reliability. It does
not by itself say whether those results are fast or slow — that relationship is captured
by the HPF value itself.

---

### 3. Diversity

**What it measures:** how broadly the boat races, weighted by racing variant.

**Derivation:** percentile rank of a variant-weighted opponent exposure score. For each
distinct *(opponent, variant)* pair encountered in recent races, the variant's weight is
added to the score:

| Variant | Weight |
|---|---|
| Non-spinnaker | 0.8 |
| Spinnaker | 1.0 |
| Two-handed | 1.2 |

The same opponent met in two different variants counts twice. Racing against a wider
field, or in more demanding variants, scores higher.

**Interpretation:** a high diversity score means the boat's HPF is grounded in varied
competitive experience — multiple fleets, different course types, different race lengths.
This makes the HPF more reliable as a cross-context descriptor of performance.

---

### 4. NonChaotic

**What it measures:** whether performance variation is driven by racing conditions
rather than boat or crew unpredictability.

**Derivation:** percentile rank of the weighted Pearson correlation between a boat's
absolute residual and the race's fleet dispersion (weighted IQR of corrected times across
the whole fleet that day). High positive correlation means the boat's large residuals
coincide with days when the whole fleet was scattered — its inconsistency is
conditions-driven, not intrinsic. Low or negative correlation means the boat has bad
races even on days when everyone else is sailing predictably.

Boats with fewer than 5 paired observations receive a score of 0.

**Interpretation:** a high NonChaotic score does not mean the boat is fast; it means
that when the boat underperforms, there is usually a fleet-wide reason. Combined with
high Consistency this is the ideal — the boat is reliable, and its rare off days happen
in universally difficult races.

---

### 5. Stability

**What it measures:** whether the boat's performance trend is level or declining.

**Derivation:** percentile rank of an asymmetric slope penalty from a weighted linear
regression of residuals against race date. The penalty is:

- **Level trend** (slope ≈ 0): no penalty, ranks best
- **Improving trend** (negative slope — boat getting faster relative to the fleet):
  half the penalty of an equivalent declining trend
- **Declining trend** (positive slope — boat getting slower): penalised most heavily,
  as this suggests the current HPF may already be stale

**Interpretation:** a high stability score means the boat's relationship to its HPF is
settled over the analysis horizon. A low score means the boat is in transition — new
crew, new sails, ageing hull, or ownership change. An improving trend is scored
leniently because a boat getting faster is usually good news; the HPF will self-correct
as more data accumulates.

---

## Spoke Order

Spokes are arranged on the pentagon to place conceptually unrelated measures adjacent
to each other, which prevents correlated measures from inflating the visible area:

```
Frequency — Consistency — Diversity — NonChaotic — Stability
```

(Pentagon, equally spaced at 72°.)

---

## Overall Score

The overall score is the normalised area of the radar polygon:

```
area     = (1/2) × sin(2π/n) × Σ(r_i × r_{i+1 mod n})

score    = area / area_max
         = area / ((n/2) × sin(2π/n))
```

A perfect pentagon (all spokes = 1) gives score = 1.0. Any spoke at zero collapses the
two adjacent triangles, so a single persistent weakness loses disproportionately more
area than the spoke value alone would suggest. This is intentional — a single persistent
weakness is genuinely costly in racing.

The overall score is displayed as a secondary label beneath the chart. The shape is the
primary insight; the number is a convenience summary for sorting and comparison.

---

## Minimum Race Threshold

A boat must have at least **3 distinct races** in the last 12 months to appear in the
fleet ranking at all. Below this threshold no spoke scores are shown.

The NonChaotic spoke additionally requires **5 paired observations** (boat residual
plus fleet dispersion for the same race); below this it scores 0.

---

## Presentation

### Single Boat

Display the radar pentagon with each spoke labelled. Show the overall score below the
chart. Raw values (MSR, slope, correlation, etc.) are available in a tooltip or
expandable panel for users who want to understand the derivation.

### Boat Comparison

Overlay two boats as two polygons in contrasting colours with transparency. The
shape difference is immediately legible. The overall score for each is shown in the
legend.

### Series View Integration

The performance profile naturally extends the series view (HPF per boat per race).
A small radar thumbnail alongside each boat's HPF row gives the handicapper an
immediate read on whether the HPF is backed by consistent, reliable evidence or is
a noisy estimate from a variable or infrequently-racing boat.

---

## Implementation Notes

- All profile calculations are **derived layer** — pure functions of the per-race
  residuals and race dispersion metrics already produced by the HPF optimisation.
  They belong in a `PerformanceProfileBuilder` service class, not in the optimisation
  layer.
- The `BoatHpf` record (final output of the optimisation) already carries per-race
  residuals. The profile builder consumes those records plus the race dispersion
  metrics from `RaceDispersion`.
- No new raw data is required. No new ingestion logic is required.
- The radar chart is rendered client-side in Plotly.js using the `scatterpolar`
  trace type.
