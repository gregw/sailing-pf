# Boat Performance Profile

## Overview

In addition to the HPF value itself, the HPF analysis produces enough information to
characterise *how* a boat is being sailed, not just how fast it is. This is captured
as a **Performance Profile** -- a set of measures derived from the per-race residuals
produced by the HPF optimisation, each reduced to a score in [0, 1] and presented as
a radar (spider) chart.

The profile is a companion to the HPF value, not a replacement for it. HPF answers
"how fast is this boat on average". The performance profile answers "how reliably and
consistently is this boat sailed relative to that average".

---

## Residuals

All profile measures are derived from the per-race log-residuals already computed by
the HPF optimisation (Step 19 of the processing pipeline):

```
residual_i = log(elapsedTime_i) + log(HPF_boat) - log(T_race_i)
```

A negative residual means the boat was faster than its HPF predicts in that race.
A positive residual means it was slower. All measures below operate in log space,
which ensures that being 10% fast and 10% slow are treated symmetrically.

All residual statistics are **weighted** -- the same entry weights produced by the
optimisation are used here, so outlier races already down-weighted in the HPF
calculation are similarly down-weighted in the profile.

---

## The Five Spokes

### 1. Consistency

**What it measures:** how tightly the boat's results cluster around its HPF across
all races in scope.

**Derivation:** weighted IQR of log-residuals, mapped through an exponential decay:

```
consistency = exp(-k_consistency * weighted_IQR)
```

The decay constant `k_consistency` is tuned so that the median boat in the fleet
scores approximately 0.5. A boat with a very tight residual distribution (IQR near
zero) approaches 1.0; a highly scattered boat approaches 0.

**Interpretation:** a high consistency score means the boat produces similar results
race after race. This reflects stable crew, preparation, and boat reliability. It does
not by itself say whether those results are fast or slow -- that is captured by the
Ceiling spoke.

---

### 2. Ceiling

**What it measures:** how close to the boat's potential the typical result sits --
i.e. whether the boat regularly sails to its HPF or habitually under-performs it.

**Derivation:** weighted median of log-residuals, mapped through an exponential decay
applied only to the positive (slower-than-HPF) side:

```
ceiling = exp(-k_ceiling * max(0, weighted_median_residual))
```

A boat whose median result is at or below its HPF (median residual <= 0) scores 1.0
on this spoke. A boat whose typical result is consistently slower than its HPF scores
progressively lower.

A boat whose median residual is negative (consistently faster than HPF) should trigger
an HPF revision rather than receiving a ceiling score above 1.0. The score is capped
at 1.0.

**Interpretation:** a low ceiling score means the boat has more potential than its
typical results show. Combined with high consistency, this suggests the HPF itself
may be set conservatively. Combined with low consistency, it suggests variable crew
or preparation.

---

### 3. Stability

**What it measures:** whether the boat's performance level is settled over time, or
drifting.

**Derivation:** fit a weighted linear regression of log-residuals against race date.
The slope measures drift in log-residual per unit time. Map the absolute slope to [0,1]:

```
stability = exp(-k_stability * abs(slope))
```

Both improving and declining trends reduce the stability score. A flat trend (slope
near zero) scores near 1.0.

**Interpretation:** a high stability score means the boat's relationship to its HPF
is consistent over the analysis horizon. A low score means the boat is in transition
-- new crew, new sails, ageing hull, ownership change. In this case the aggregate HPF
is a less reliable descriptor of current capability, and a rolling HPF (recent window
only) may be more useful. Stability is shown as a dashed spoke in the chart to signal
that it is a meta-measure (how much to trust the HPF) rather than a direct sailing
quality measure.

---

### 4. Weather Independence

**What it measures:** how much of the boat's residual variation is explained by
race-level conditions vs. being specific to the boat.

**Derivation:** the HPF optimisation already computes a weighted IQR per race
(the race dispersion metric from Step 13). Compute the correlation between the
boat's absolute residual in each race and that race's dispersion score. High
correlation means the boat's bad races tend to be races where everyone was scattered
-- weather, tide gates, short courses. Low correlation means the boat is inconsistent
even in clean, well-behaved races.

Map to [0,1]:

```
weather_independence = (1 + correlation) / 2
```

where correlation is the Pearson correlation between |residual_i| and race_dispersion_i,
computed over all races for the boat with sufficient fleet size.

**Interpretation:** a high score means the boat's inconsistency is largely externally
driven -- bad luck in bad races, not a boat or crew problem. A low score means the
boat is scattering even when the rest of the fleet is sailing predictably.

---

### 5. Sample Confidence

**What it measures:** how much statistical weight sits behind the other four spokes.
This is not a sailing quality measure -- it is a reliability indicator for the profile
as a whole.

**Derivation:** a function of total weighted race count and recency of data:

```
confidence = 1 - exp(-k_confidence * effective_race_count)
```

where `effective_race_count` is the sum of entry weights across all races for the boat
(not a raw count), further discounted for races older than the analysis horizon.

**Presentation:** this spoke is always rendered in a distinct visual style (dashed
line, muted colour) to make clear it is a confidence indicator, not a performance
dimension. It is included in the polygon area calculation so that a boat with few
races is penalised in the overall score -- a beautiful shape on three races should
not outscore a solid shape on thirty.

---

## Spoke Order

Spokes are arranged to place conceptually unrelated measures adjacent to each other,
which prevents correlated measures from inflating the visible area:

```
Consistency  --  Ceiling  --  Weather Independence  --  Sample Confidence  --  Stability
```

(Pentagon, equally spaced at 72 degrees.)

---

## Overall Score

The overall score is the area of the radar polygon, normalised by the maximum possible
area (all spokes = 1):

```
area = (1/2) * sin(2*pi/n) * sum(r_i * r_{i+1 mod n})

score = area / area_max
      = area / ((n/2) * sin(2*pi/n))
```

This area metric penalises unevenness: a boat with one spoke at zero loses
disproportionately more area than the spoke value alone would suggest, because the
adjacent triangles collapse. This is the correct behaviour -- a single persistent
weakness is genuinely costly in racing.

The overall score is displayed as a secondary label beneath the chart. The shape
is the primary insight; the number is a convenience summary for sorting and comparison.

---

## Fleet Normalisation

The decay constants (k_consistency, k_ceiling, k_stability, k_confidence) and the
weather independence mapping can be calibrated in two ways:

- **Absolute:** the [0,1] mapping is fixed based on theoretical maxima. Scores are
  stable as the fleet grows but may compress into a narrow band if the fleet is
  homogeneous.
- **Fleet-relative:** the [0,1] mapping is based on percentile rank within the current
  fleet. More discriminating and visually interesting, but scores change as the fleet
  grows.

The recommended approach is absolute mapping with decay constants tuned against the
full fleet distribution on first deployment, then held fixed. This makes scores
comparable across time and across fleets.

---

## Minimum Race Threshold

Spokes other than Sample Confidence require a minimum number of races to be
meaningful:

| Spoke | Minimum races |
|---|---|
| Consistency | 5 |
| Ceiling | 5 |
| Stability | 10 (needs enough temporal spread for regression) |
| Weather Independence | 8 (needs enough variance to compute correlation) |
| Sample Confidence | 1 |

Below the minimum, a spoke is rendered as a dashed line at 0 and excluded from the
area calculation. The Sample Confidence spoke remains active at all times.

---

## Presentation

### Single Boat

Display the radar polygon with each spoke labelled. Show the overall score below the
chart. Optionally show the raw values (weighted IQR, median residual, slope, etc.)
in a tooltip or expandable panel for users who want to understand the derivation.

### Boat Comparison

Overlay two boats as two polygons in contrasting colours with transparency. The
shape difference is immediately legible. The overall score for each is shown in the
legend.

### Series View Integration

The performance profile naturally extends the series view (HPF per boat per race).
A small radar thumbnail alongside each boat's HPF row gives the handicapper an
immediate read on whether the HPF is backed by consistent, reliable evidence or is
a noisy estimate from a variable boat.

---

## Implementation Notes

- All profile calculations are **derived layer** (deterministic derived) -- they
  are pure functions of the per-race residuals and race dispersion metrics already
  produced by the HPF optimisation. They belong in a `PerformanceProfileBuilder`
  service class, not in the optimisation layer.
- The `BoatHpf` record (final output of the optimisation) already carries per-race
  residuals. The profile builder consumes those records plus the race dispersion
  metrics from `RaceDispersion`.
- No new raw data is required. No new ingestion logic is required.
- The radar chart is rendered client-side in Plotly.js. Plotly has native support
  for polar/radar charts (`scatterpolar` trace type).
