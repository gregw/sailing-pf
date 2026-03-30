# Object Model Architecture

## Previous Implementation

There was a previous Java prototype in the repository. It was a deliberate **quick-and-dirty exploratory prototype** — written to understand the domain, not as a production design. The prototype code has been deleted; only the domain knowledge it embodied influenced the new design. The specific design problems it exhibited are listed below as guidance on what to avoid.

---

## Known Problems in the Previous Implementation

### 1. Static Global Registries
Classes like `Boat` use a static `ArrayList` as a global singleton registry (e.g. `__byId`). This makes it impossible to run multiple optimisation scopes simultaneously, breaks unit testing, and creates hidden shared state. **Do not replicate this pattern.**

### 2. Raw and Derived Data Mixed in the Same Class
Mutable derived fields (computed handicaps, race counts, running averages) sit alongside immutable raw fields in the same class. For example, `Boat` holds both the raw `_name`/`_sailNumber` (final) and mutable `_spinnakerHC`/`_racedSpinnaker` (derived). These have fundamentally different lifecycles and must be separated.

### 3. Back-References Creating Circular Graphs
`Boat` holds a `List<Entry>` — a direct back-reference from parent to child. This creates circular object graphs that cause problems for serialisation (Jackson infinite recursion), testing, and reasoning about object ownership. **Navigation from parent to children must go through index objects, not fields on the parent.**

### 4. Domain Logic in the Wrong Place
The `add()` method on `Boat` contains MYC-specific logic for tracking the latest handicap date. Club-specific concerns must not live in a generic domain object.

### 5. Analysis Logic in Domain Objects
`addRaced()` computes a running average handicap inline on `Boat`. Analysis and optimisation logic belongs in dedicated service classes, not in the entities being analysed.

### 6. Save/Restore Pattern
`saveHC()` / `restoreHC()` exist to support optimisation iterations. This is a symptom of mutable derived state living on the raw object. In the new design, optimisation state lives in dedicated mutable working objects; the raw layer is never mutated.

---

## New Architecture Principles

### Layer Overview

The architecture has four layers with a strict one-way dependency:

```
Optimised derived  →  Deterministic derived  →  Raw
Index              →  Raw
Raw                →  (nothing in this project)
```

---

### Layer 1: Raw — Records, Always Persisted, Immutable

The raw layer captures data exactly as ingested from source systems. It is persisted as JSON files (one per top-level entity). Once stored, raw data is never mutated — updates create a new record with updated fields.

- All raw layer entities are **Java records** — immutability, compact syntax, and value semantics.
- Raw records hold **no back-references** and **no derived fields**.
- Raw records know nothing about any other layer.
- Top-level entities implement the `Loadable` interface for dirty-tracking via `loadedAt()` timestamp.

**Top-level records** (one JSON file each):
- `Boat` — id, sailNumber, name, designId, clubId, aliases (List\<TimedAlias\>), altSailNumbers, certificates (List\<Certificate\>), sources, lastUpdated
- `Race` — id, clubId, seriesIds, date, number, name, handicapSystem, offsetPursuit, divisions (List\<Division\>), source, lastUpdated
- `Club` — id, shortName, longName, state, aliases, topyachtUrls, series (List\<Series\>)
- `Design` — id, canonicalName, makerIds, aliases, sources, lastUpdated

**Embedded records** (nested inside their parent, not persisted separately):
- `Certificate` — system (IRC/ORC/AMS), year, value (TCF), nonSpinnaker, twoHanded, club, windwardLeeward, certificateNumber, expiryDate. Embedded in `Boat`.
- `Division` — name, finishers (List\<Finisher\>). Embedded in `Race`.
- `Finisher` — boatId, elapsedTime (Duration), nonSpinnaker, certificateNumber. Embedded in `Division`.
- `Series` — id, name, isCatchAll, raceIds. Embedded in `Club`.
- `TimedAlias` — name, from (LocalDate), until (LocalDate). Embedded in `Boat`.
- `Maker` — id, canonicalName, aliases. Stored in `catalogue/makers.json`.

**Supporting records:**
- `Factor` — value (double) + weight (double). Used throughout the analysis layer; provides static methods `apply()`, `compose()`, `aggregate()`.
- `WeightedInterval` — duration + weight. Used in race analysis.

---

### Layer 2: Deterministic Derived — Records, Held in AnalysisCache

The deterministic derived layer is computed from the raw layer by functions that are deterministic given the same raw data and configuration parameters. Results are held in volatile fields in `AnalysisCache` — not persisted to disk, but recomputed on startup and after each importer run.

- All derived types are **Java records** — immutable once computed.
- Recomputation is triggered by `AnalysisCache.refresh()` after importer runs or configuration changes.

**Comparison analysis** (produced by `HandicapAnalyser`):
- `ComparisonResult` — key (ComparisonKey), pairs (DataPair[]), trimmedPairs (outlier-removed), fit (LinearFit)
- `ComparisonKey` — identifies a handicap comparison (system A/B, variant A/B, year A/B); factory methods for all comparison types
- `DataPair` — boatId, x (TCF), y (TCF)
- `LinearFit` — slope, intercept, r², se, n, xMean, ssx; methods: predict(x), inverse(), weight(x)

**Conversion graph** (produced from ComparisonResults):
- `ConversionGraph` — directed graph of system×year×variant nodes connected by LinearFit edges (minimum R² = 0.75)
- `ConversionNode` — system, year, nonSpinnaker, twoHanded
- `ConversionEdge` — from node, to node, fit

**Reference factors** (produced by `ReferenceNetworkBuilder` via DFS of the ConversionGraph):
- `BoatReferenceFactors` — spin (Factor), nonSpin (Factor), twoHanded (Factor), with generation tracking per variant
- `Factor` — value + weight pair with static composition methods

These are the **anchors** for the future HPF optimisation layer.

---

### Layer 3: Index — Derived, Held in AnalysisCache

Navigation that would require back-references on raw records (e.g. "all races for a boat") is handled by **index maps** in `AnalysisCache` rather than collection fields on parent entities. Indexes are derived deterministically from raw data and rebuilt via `AnalysisCache.refreshIndexes()`.

```java
// Rather than: boat.getEntries()  ← do not do this
// Use:         cache.raceIdsByBoatId().get(boat.id())
```

Current indexes (volatile `Map` fields in AnalysisCache):
- `boatIdsByDesignId` — designId → Set of boatIds
- `raceIdsByBoatId` — boatId → Set of raceIds
- `seriesIdsByBoatId` — boatId → Set of seriesIds

---

### Layer 4: Optimised Derived — Not Yet Implemented

The optimised derived layer will be produced by the HPF optimisation (pipeline Steps 13–19). It will be **non-deterministic** in the sense that its output depends on configuration (scope, λ, convergence threshold). It will never be persisted — always recomputed on demand.

- Use **ordinary mutable classes** for working objects updated during alternating least squares iterations.
- Use **records** for final output snapshots once optimisation converges.
- Working objects hold references to raw records and deterministic derived records as fixed inputs; they never mutate those inputs.

---

### Persistence Boundary Summary

| Layer | Persisted? | Condition |
|---|---|---|
| Raw | Always (JSON files) | Source of truth |
| Deterministic derived | No (held in AnalysisCache) | Recomputed on startup and after imports |
| Index | No (held in AnalysisCache) | Recomputed on demand |
| Optimised derived | Never | Not yet implemented |

### Persistence Layout

```
hpf-data/                          (resolved via HPF_DATA env, ./hpf-data, or ~/.hpf-data)
  boats/{boatId}.json
  designs/{designId}.json
  clubs/{clubId}.json
  races/{clubId}/{seriesSlug}/{raceId}.json
  catalogue/makers.json
  config/
    admin.yaml                     (importer schedule, thresholds, configurable weights)
    aliases.yaml                   (read-only seed: boat/design aliases, sail number redirects)
    clubs.yaml                     (read-only seed: club metadata, TopYacht URLs)
    design.yaml                    (read-only seed: exclusions, design overrides)
    exclusions.json                (mutable: excluded boat/design/race IDs, managed by admin UI)
```

---

### Service Layer

Analysis and optimisation logic lives in dedicated service classes, not in entity objects:

**Importers** (populate Layer 1):
- `SailSysBoatImporter` — imports boats and IRC certificates from SailSys boat API/files
- `SailSysRaceImporter` — imports races, divisions, finishers from SailSys race API/files
- `TopYachtImporter` — scrapes TopYacht HTML; handles multi-page merge, caption-based variant detection
- `OrcImporter` — fetches ORC certificate feed XML; creates certificates with variant flags
- `AmsImporter` — scrapes AMS certificate listings from raceyachts.org
- `BwpsImporter` — imports BWPS (CYCA) race results

**Analysis** (Layers 2–3):
- `HandicapAnalyser` — mines paired observations across boats to produce `ComparisonResult` with `LinearFit`
- `ConversionGraph` — directed graph of system×year×variant conversions
- `ReferenceNetworkBuilder` — DFS traversal of ConversionGraph to compute `BoatReferenceFactors` (Steps 8–12)
- `AnalysisCache` — holds all derived data in volatile fields; provides `refresh()`, `refreshReferenceFactors()`, `refreshIndexes()`

**Server**:
- `HpfServer` — Jetty 12.1 embedded server entry point (port 8080)
- `ImporterService` — manages importer lifecycle, background execution, scheduling, and admin configuration (from `admin.yaml`)
- `AdminApiServlet` — REST API for boats/designs/races/importers/stats (paginated, with search/filter)
- `AnalysisServlet` — REST API for comparison analysis results
- `StaticResourceServlet` — serves admin frontend HTML/JS/CSS from classpath resources

**Store**:
- `DataStore` — central persistence; reads/writes JSON files; manages in-memory maps; Jaro-Winkler fuzzy matching for boat/design names
- `AliasSeedLoader` — reads `aliases.yaml` for boat/design name aliases and sail number redirects
- `ClubSeedLoader` — reads `clubs.yaml` for club metadata and TopYacht URLs
- `DesignCatalogueLoader` — reads `design.yaml` for exclusions and design overrides
- `IdGenerator` — pure normalisation utilities for generating entity IDs
- `RemoveNonMeasurementCertificates` — housekeeping utility to strip PHS/CBH certificates

Entity records and classes hold data. Services hold behaviour.
