# Processing Pipeline

## Overview

This document describes the full processing pipeline for the Australian Yacht Racing Elapsed Time Database. The pipeline is divided into two phases: **Data Preparation and Reference Network Construction** (steps 1–12) and **HPF Calculation** (steps 13–19).

---

## Phase 1: Data Preparation and Reference Network Construction (Implemented)

All steps in Phase 1 are implemented and operational. They are run via the admin webapp's
import management page or via scheduled runs configured in `admin.yaml`.

### Step 1: Import ORC Certificate Data (`OrcImporter`)

Fetch the ORC Australian certificate list from:
```
https://data.orc.org/public/WPub.dll?action=activecerts&CountryId=AUS
```

For each certificate, fetch the full certificate detail page using `dxtID`. Parse GPH from the
HTML (regex on `filecode="GPH"`) and convert to TCF via `600 / GPH`. Detect certificate variant
from `CertType` and `FamilyName` fields:

| CertType | FamilyName | Flags set |
|---|---|---|
| 1 | — | (IRC+ORC international) |
| 2 | ORC Standard | (none — standard international) |
| 3 | ORC Standard | `club=true` |
| 8 | Double Handed | `twoHanded=true` |
| 9 | Double Handed | `twoHanded=true`, `club=true` |
| 10 | Non Spinnaker | `nonSpinnaker=true` |
| 11 | Non Spinnaker | `nonSpinnaker=true`, `club=true` |

Creates/updates `Boat`, `Design`, and embedded `Certificate` records. Certificate is stored
with `system="ORC"` and the `dxtID` as `certificateNumber` for idempotency.

---

### Step 2: Import AMS Certificates (`AmsImporter`)

Scrape AMS certificate listings from raceyachts.org. Creates `Certificate` records with
`system="AMS"`. Includes non-spinnaker and two-handed variants.

---

### Step 3: Import SailSys Boat Records (`SailSysBoatImporter`)

Import boats from SailSys, either from pre-downloaded JSON files (`--local` mode) or via
HTTP API with configurable throttling. Extracts IRC certificates from the `handicaps[]` array
where `definition.shortName` is `IRC` or `IRC SH`. Maps `IRC SH` to `twoHanded=true`.
`make`/`model` fields are normalised for design name derivation; ORC `Class` is preferred.

---

### Step 4: Import SailSys Races (`SailSysRaceImporter`)

Scan SailSys race records starting from `nextSailSysRaceId` (configured in `admin.yaml`).
Supports local file mode and HTTP API mode. For each completed race (non-null
`lastProcessedTime`), extracts:

- Race metadata (club, series, date, handicap system, divisions)
- Per-boat entries: sail number, name, elapsed time, `nonSpinnaker` flag
- Certificate inference from `handicapCreatedFrom` in `calculations[]` — the handicap actually
  used for scoring. Inferred certificates include the `nonSpinnaker` flag from the race entry.

Unknown boats are resolved via fuzzy matching (Jaro-Winkler, threshold configurable in
`admin.yaml`) or created as new `Boat` instances. The SailSys boat API is fetched on demand
for design information. Clubs are matched via `clubs.yaml` seed data.

---

### Step 5: Import TopYacht Races (`TopYachtImporter`)

Scrape TopYacht HTML result pages from club URLs curated in `clubs.yaml`. For each result page:

1. Parse the group caption to detect handicap system and variant flags. The caption parser
   detects `ORC`, `AMS`, and `IRC` as base system keywords, then scans subsequent tokens:
   - `NS` → `nonSpinnaker=true`
   - `WL` → `windwardLeeward=true`
   - `DH`, `2HD`, `SH` → `twoHanded=true`
   - Underscores are normalised to spaces before splitting (handles `ORC_AP`, `ORC_WL` etc.)
2. Only measurement systems (IRC, ORC, AMS) have their AHC values stored as inferred
   certificates; PHS and other systems are stored for elapsed time only.
3. Multi-page results (same race with different handicap systems) are merged into a single
   `Race` with multiple divisions. The merge path uses `isMeasurementHandicapSystem()` to
   filter which handicap values to keep.

Inferred certificates include `nonSpinnaker`, `twoHanded`, and `windwardLeeward` flags from
the parsed caption.

---

### Step 6: Import BWPS Races (`BwpsImporter`)

Import BWPS race results from the Cruising Yacht Club of Australia. IRC-based races providing
boat identity, elapsed times, and IRC handicaps used for scoring.

---

### Step 7: Build Indexes (`AnalysisCache.refreshIndexes()`)

Build navigation indexes from raw data:
- `boatIdsByDesignId` — designId → Set of boatIds
- `raceIdsByBoatId` — boatId → Set of raceIds
- `seriesIdsByBoatId` — boatId → Set of seriesIds

These indexes replace back-references that would otherwise be needed on entity records.

---

### Step 8: Build Conversion Graph (`HandicapAnalyser`)

`HandicapAnalyser.analyseAll()` mines all boats for paired handicap observations. For each
boat holding certificates in multiple systems, years, or variants, it emits `DataPair` records
linking the two TCF values. Pairs are grouped by `ComparisonKey` (e.g. "IRC spin 2024 → ORC
spin 2024", "IRC spin 2023 → IRC spin 2024").

For each comparison key with enough pairs, a `LinearFit` is computed via least squares
regression. Outliers beyond a configurable sigma threshold (default 2.5) are trimmed and the
fit recomputed.

`ConversionGraph.from(comparisons)` builds a directed graph where:
- **Nodes** are `ConversionNode(system, year, nonSpinnaker, twoHanded)` tuples
- **Edges** carry the `LinearFit` (minimum R² = 0.75 required for inclusion)
- The graph enables conversion between any two connected system×year×variant nodes

---

### Step 9: Compute Reference Factors (`ReferenceNetworkBuilder`)

`ReferenceNetworkBuilder.build(store, graph, targetYear)` computes a `BoatReferenceFactors`
for every boat. The target is the IRC node for `targetYear` (configurable, defaults to the
max issued IRC cert year in the data).

**Certificate-based factors (generation 0):**
For each boat's certificates, perform DFS through the ConversionGraph to find the shortest
weighted path from the certificate's node to the target IRC node. Certificate base weight is
1.0, with multiplicative discounts:
- Club certificate: × `clubCertificateWeight` (default 0.9, configurable in `admin.yaml`)
- Windward/leeward: × 0.8
- Certificate age: degrades by 0.2 per year beyond the target year
- Each graph hop: weight scaled by the edge's `LinearFit.weight()`

Where multiple paths exist, the highest-weight path wins. Three factors are computed
independently: spinnaker, non-spinnaker, and two-handed.

---

### Step 10: Aggregate to Design Level

For each design with multiple boats having reference factors, aggregate into a design-level
factor using `Factor.aggregate()` (weighted mean). Design-level factor weight is scaled by
`DESIGN_FACTOR_WEIGHT` (0.85).

---

### Step 11: Propagate via Race Co-participation

For boats without reference factors, scan all races where they competed against boats that
do have factors. Estimate an implied factor from elapsed time ratios, weighted by the
reference boats' factor weights. Propagation weight is scaled by `PROPAGATION_FACTOR_WEIGHT`
(0.7).

---

### Step 12: Iterate Until Convergence

Repeat steps 10 and 11 until no new factors are assigned (max 20 iterations). Each iteration
may produce new design-level factors and new boat-level propagated factors. At convergence,
every reachable boat has a `BoatReferenceFactors` with spin, nonSpin, and/or twoHanded
factors and associated weights.

---

## Phase 2: HPF Calculation

### Step 13: Compute Initial Reference Time per Race

For each race, for each boat entry, compute a **factor-corrected elapsed time**:

```
correctedTime = elapsedTime / referenceFactor
```

Take the **weighted median** of these values across all boats in the race, weighted by each boat's reference factor weight. This is the initial **reference time** T₀ for the race — the estimated elapsed time a 1.000 HPF boat would have taken to complete the course.

Use weighted median rather than mean because elapsed time distributions are right-skewed — slow outliers due to gear failure, bad wind holes or tactical errors are common and should not distort the baseline.

Record the **weighted IQR** of the corrected times as a first-pass measure of race dispersion. Races with high IQR relative to T₀ are candidates for down-weighting in subsequent steps.

---

### Step 14: Compute Initial Per-Boat HPF Estimates

For each race entry, compute the boat's initial HPF estimate for that race:

```
HPF_race = T₀ / elapsedTime
```

This is the handicap the boat would have needed to equal the median corrected time in that race. Aggregate across all races for a boat using a **weighted mean in log space**:

```
log(HPF_boat) = Σ( w_r × log(HPF_race) ) / Σ(w_r)
```

Where w_r is the race's aggregate reference weight — the sum of reference factor weights of all boats in that race. Working in log space ensures that being 10% fast and 10% slow are treated symmetrically, which is the correct prior before asymmetric weighting is applied in Step 15.

---

### Step 15: Assign Initial Race and Entry Weights

For each race, compute a **race weight** based on:

- Total reference factor weight of all boats in the race — more and better-anchored boats give higher weight
- IQR / T₀ ratio — higher dispersion gives lower weight, reflecting a less reliable race

For each boat entry within a race, compute an **entry weight** based on:

- The boat's reference factor weight
- How far the boat's corrected time deviates from T₀ — entries more than k×IQR from the median are down-weighted (k ≈ 2.0, to be tuned)
- **Asymmetry principle**: entries where the boat was surprisingly fast relative to T₀ are down-weighted more aggressively than entries where the boat was slow. It is essentially impossible for a boat to genuinely sail faster than its potential; an apparently outstanding result in a small fleet is more likely to reflect poor performance by others or a weather/tide gate than genuine exceptional performance.

---

### Step 16: Alternating Least Squares Optimisation (Log Space)

The two unknowns are:

- **HPF** for each boat — one value per boat, stable across all races in the optimisation scope
- **T** for each race — one reference time per race

The objective is to minimise across all entries in scope:

```
Σ w_entry × ( log(elapsedTime) + log(HPF_boat) - log(T_race) )²
```

plus a **regularisation term** pulling each boat's HPF toward its reference factor:

```
+ λ × referenceFactor_weight × ( log(HPF_boat) - log(referenceFactor) )²
```

λ is a global tuning parameter controlling the overall strength of the anchor. The per-boat pull is modulated by each boat's individual reference factor weight — boats with high weight stay close to their reference factor value; boats with low weight float more freely toward what the race data suggests.

**Alternating steps:**

- **A — Fix HPF, solve for T per race:** Each T_race has a closed-form weighted mean solution:
  ```
  log(T_race) = Σ( w_entry × (log(elapsedTime) + log(HPF_boat)) ) / Σ(w_entry)
  ```
- **B — Fix T, solve for HPF per boat:** Each HPF_boat has a closed-form solution combining the race evidence with the regularisation pull:
  ```
  log(HPF_boat) = [ Σ( w_entry × (log(T_race) - log(elapsedTime)) ) + λ × referenceFactor_weight × log(referenceFactor) ]
                  / [ Σ(w_entry) + λ × referenceFactor_weight ]
  ```

Iterate A and B until convergence — defined as the maximum change in any HPF value across an iteration falling below a threshold (e.g. 0.0001).

---

### Step 17: Recompute Weights and Iterate

After convergence of Step 16, recompute entry weights using the residuals from the fitted model:

```
residual = log(elapsedTime) + log(HPF_boat) - log(T_race)
```

A positive residual means the boat was slower than expected; a negative residual means it was faster. Re-apply the asymmetry principle: large positive residuals are down-weighted moderately; large negative residuals are down-weighted more aggressively.

Flag races where a large fraction of entries have high absolute residuals — this signals a weather/tide gate or other race-level anomaly. In this case, down-weight the entire race rather than individual entries.

Return to Step 16 with the updated weights and re-run to convergence. Repeat until weights stabilise across outer iterations (typically 3–5 outer iterations are sufficient).

---

### Step 18: Scope of Optimisation

The optimisation can be run at several scopes, from narrowest to broadest:

- **Single race** — useful for inspection and debugging; T is fixed, only HPF values float
- **Single series** — the natural unit for initial handicap allocation; T floats per race, HPF per boat is shared across all races in the series
- **Single club** — all series at a club within a season; boats appearing in multiple series receive a consistent HPF across them
- **Full fleet** — all clubs and series in the database; maximises the data available for each boat, with the regularisation term preventing drift between disconnected sub-graphs

The regularisation term is essential for full-fleet scope: without it, the solution is only identified up to a global scale factor and sub-graphs with no cross-club boats can drift arbitrarily.

---

### Step 19: Output

For each boat in the optimisation scope, emit:

- **HPF** — the Historical Performance Factor: the back-calculated handicap the boat would have needed to be equal-time with a 1.000 reference boat, averaged across its racing history in scope
- **HPF confidence** — derived from the total weighted race count and consistency of results across races
- **HPF vs reference factor delta** — how far the optimised HPF drifted from the reference factor anchor; large deltas on boats with high reference factor weight warrant investigation
- **Per-race residuals** — the deviation of each race result from the fitted model, for visualisation of boat consistency over time

HPF is explicitly a **historical performance measure**, not a future handicap allocation. It is intended to inform the allocation of initial handicaps at season start or when a new boat joins a series, and is not a replacement for the way individual clubs age or adjust handicaps between races.
