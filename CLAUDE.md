# CLAUDE.md — Australian Yacht Racing Elapsed Time Database

This file orients Claude Code to the project. Read it before making any changes.
All linked documents are in the `.claude/` directory alongside this file.

---

## What This Project Is

An online database of elapsed times from Australian yacht racing, with statistical analysis
tools that compute an **HPF (Historical Performance Factor)** for each boat.

**HPF** is the back-calculated handicap a boat would have needed, averaged across its racing
history, to have been equal-time with a hypothetical 1.000 reference boat. It is a historical
performance measure — not a handicap system.

**Primary use case:** Informing the allocation of initial handicaps at season start, or when a
new boat joins a series. This tool explicitly does not replace how clubs age or adjust handicaps
between races day-to-day.

---

## Project Documentation

Read these files before working in any given area:

| File | What it covers |
|---|---|
| [project_description.md](src/main/resources/admin/guide/project_description.md) | Full overview: data collection, reference (candle) network, HPF estimation, website |
| [object_model_architecture.md](src/main/resources/admin/guide/object_model_architecture.md) | Java object model, layering principles, what NOT to replicate from the old prototype |
| [processing_pipeline.md](src/main/resources/admin/guide/processing_pipeline.md) | Step-by-step pipeline from data ingestion through to HPF output |
| [id_strategy.md](src/main/resources/admin/guide/id_strategy.md) | How entities are keyed and disambiguated — read before touching IDs |
| [data_sources_and_formats.md](src/main/resources/admin/guide/data_sources_and_formats.md) | SailSys API, TopYacht HTML, ORC feed — field-level detail |
| [presentation_layer.md](src/main/resources/admin/guide/presentation_layer.md) | REST API design and browser-based charting frontend |
| [error_bars.md](src/main/resources/admin/guide/error_bars.md) | Error bar math, sigma_0 calibration, display rules |
| [handicaps.txt](src/main/resources/admin/guide/handicaps.txt) | Domain knowledge notes on yacht racing handicap systems |

## Background

This project was originally prototyped in a repository called sailing-handicaps. The prototype
code has been deleted; only the domain knowledge it embodied influenced the new design. Raw
SailSys files have been processed and only public data (names, sail numbers, elapsed times,
measurement handicaps) extracted and stored. The original downloads have been deleted.

---

## Technology Stack

- **Language:** Java 21
- **Server:** Jetty 12.1 embedded (plain Jakarta servlets — no Spring, no Spring Boot)
- **Data access:** Jackson JSON/YAML files inflated to Java records; file-per-entity persistence
- **HTTP client:** Jetty HttpClient (for importer API calls and web scraping)
- **HTML parsing:** JSoup (TopYacht HTML scraping)
- **Fuzzy matching:** Apache Commons Text (Jaro-Winkler similarity for boat/design name matching)
- **Frontend:** Plain JavaScript, Plotly.js — no React, no build pipeline
- **Testing:** JUnit 5
- **Deployment target:** Google Cloud Run or Fly.io (scale-to-zero)

---

## Architecture: Four-Layer Model

This is the most important architectural constraint. **Never mix layers.**

```
Optimised derived  →  Deterministic derived  →  Raw
Index              →  Raw
Raw                →  (nothing)
```

### Layer 1: Raw (always persisted, immutable)
- Ingested data exactly as received from source systems
- Java **records** — immutable, no back-references, no derived fields
- Top-level entities (one JSON file each): `Boat`, `Race`, `Club`, `Design`
- Embedded records (nested inside their parent): `Certificate` (in Boat), `Division` and `Finisher` (in Race), `Series` (in Club), `TimedAlias` (in Boat)
- Supporting records: `Factor` (value + weight pair), `Maker` (in catalogue/makers.json)

### Layer 2: Deterministic Derived (recomputed, held in AnalysisCache)
- Computed from raw data by pure functions, optionally with tuning parameters
- Java **records** — always the same output given the same raw input and parameters
- `ComparisonResult` / `DataPair` / `LinearFit` — empirical conversion fits between handicap systems
- `ConversionGraph` / `ConversionNode` / `ConversionEdge` — directed graph of system×year×variant conversions
- `BoatReferenceFactors` — per-boat reference factors (spin, nonSpin, twoHanded) with generation tracking
- Recomputed via `AnalysisCache.refresh()` after importer runs

### Layer 3: Index (derived, held in AnalysisCache)
- Provides navigation that would otherwise require back-references (e.g. "all races for a boat")
- Always recomputable from raw; correctness never depends on the persisted index
- Volatile maps in `AnalysisCache`: `boatIdsByDesignId`, `raceIdsByBoatId`, `seriesIdsByBoatId`
- Use indexes — **never** `boat.getEntries()` or similar back-references

### Layer 4: Optimised Derived (not yet implemented — see Phase 2 in processing_pipeline.md)
- Will contain HPF optimisation output — depends on configuration (scope, λ, convergence threshold)
- Mutable **classes** for working objects during iteration; **records** for final output snapshots

### Persistence boundary summary

| Layer | Persisted? |
|---|---|
| Raw | Always |
| Deterministic derived | Yes (invalidated on upstream change) |
| Index | Optional (performance only) |
| Optimised derived | Never |

---

## Key Domain Concepts

### HPF (Historical Performance Factor)
The central output. The back-calculated handicap a boat would have needed to be equal-time
with a 1.000 reference boat, averaged across its racing history. **Historical measure only.**

### References (aka "standard candles")
Boats whose performance can be independently estimated from measurement-based handicaps.
Used to anchor the optimisation. They function like standard candles in Astrophysics.

Reference factors are computed by `ReferenceNetworkBuilder` via DFS traversal of the
`ConversionGraph` — a network of empirical linear fits between handicap system×year×variant
nodes. Certificate base weight is 1.0, with multiplicative discounts:

| Modifier | Weight multiplier | Applies when |
|---|---|---|
| Club certificate | configurable (default 0.9) | ORC club, potentially IRC club |
| Windward/leeward cert | 0.8 | ORC WL course-specific certs |
| Design-level fallback | 0.85 | No individual cert; using design aggregate |
| Race propagation | 0.7 | Derived from race co-participation |

Factors degrade through DFS path traversal (each hop through the conversion graph applies
the edge's linear fit and accumulates weight penalties).

### Measurement Handicap Systems
- **IRC** — secret algorithm, RORC/UNCL managed, produces TCC. Do NOT use the RORC online
  TCC database (restricted). Acceptable sources: SailSys boat records, published race result AHC values.
  Variants: standard (spinnaker), shorthanded (IRC_SH → stored as `twoHanded=true`).
- **ORC / ORCi / ORCclub** — transparent VPP algorithm, public data feed at `data.orc.org`.
  Produces GPH (convert to TCF: `TCF = 600 / GPH`) and full VPP curves.
  Variants: standard, non-spinnaker (NS), double-handed (DH), windward/leeward (WL), club.
  TopYacht encodes variants in group captions (e.g. "ORC NS WL LO results").
- **AMS** — Australian system, Victorian-primary, 4-year recalibration cycle, no public algorithm.
  Variants: standard, non-spinnaker (AMS NS), two-handed (AMS 2HD).
- **PHS** — performance (past results) based. **Exclude PHS handicap values entirely as they may reflect proprietary information of other clubs or systems.**
  Elapsed times from PHS races are valid and used; only the PHS number itself is excluded.

### Certificate Variant Flags
Each `Certificate` record carries boolean flags that capture the certificate variant:
- `nonSpinnaker` — true for NS certificates (ORC NS, AMS NS, IRC NS)
- `twoHanded` — true for double-handed/shorthanded certs (ORC DH, AMS 2HD, IRC SH)
- `windwardLeeward` — true for ORC WL (windward/leeward course-specific) certs
- `club` — true for club-level certs (ORC club); carries configurable weight penalty (default 0.9)

---

## Data Sources

### SailSys
- API base: `https://api.sailsys.com.au/api/v1/`
- Boats: `/boats/{id}` — sequential integer IDs, brute-forced
- Races: `/races/{id}/resultsentrants/display`
- **Key field:** `handicapCreatedFrom` in `calculations[]` is the handicap actually used for
  scoring — use this, not `currentHandicaps[].value`
- `make` / `model` can bleed together; prefer ORC `Class` for design names
- **Local file mode is first priority** — large volume already downloaded
- extract only data that is public record: names, sail numbers, clubs, elapsed times, etc.

### TopYacht
- Data on many individual club websites with similar HTML structure
- `robots.txt` may block crawling; scrape from saved local copies or club-hosted pages
- `boid` in boat name links is the cross-reference to the TopYacht boat register — not persisted
- `AHC` column is the handicap used for scoring — stored only for measurement systems (IRC, ORC, AMS)
- Group captions encode handicap system and variant flags (e.g. "Div 1 ORC NS WL results")
- Multi-page results (same race, different handicap systems) are merged into a single race

### Manly Yacht Club
- For many years MYC ran their own handicap processing system and much of the data is still relevant to their current fleet.

### ORC Certificate Feed
- `https://data.orc.org/public/WPub.dll?action=activecerts&CountryId=AUS`
- Open data, no restrictions. Best source for authoritative design names (`Class` field)
- `dxtName` field groups sister ships (same hull file = same design variant)
- CertType values: 1=IRC+ORC intl, 2=ORC intl, 3=ORC club, 8=DH intl, 9=DH club, 10=NS intl, 11=NS club
- FamilyName indicates variants: "Non Spinnaker", "Double Handed"

### AMS Certificates
- Scraped from `raceyachts.org` listing
- Includes non-spinnaker and two-handed variants

### CYCA BWPS (BlueSail WorldWide Performance System)
- Publicly available club result pages from the Cruising Yacht Club of Australia
- IRC-based races; provides boat identity, elapsed times, and IRC handicaps used for scoring

---

## Entity Identity Rules

Read [id_strategy.md](.claude/id_strategy.md) fully before touching IDs. Summary:

| Entity | Key pattern | Example |
|---|---|---|
| Club | Website domain | `myc.com.au` |
| Maker | Normalised name | `farr` |
| Design | Normalised name | `j24`, `farr40` |
| Series | `clubDomain/normalisedName` | `myc.com.au/wednesday-twilight` |
| Boat | `sailnum-name-designid` | `AUS1234-raging-tp52` |
| Race | `clubDomain-date-nnnn` | `myc.com.au-2024-11-06-0001` |
| Certificate | Embedded in Boat | `orc-inferred-ns-4a1f...` or ORC dxtID |
| Finisher | Embedded in Race→Division | keyed by boatId within a division |

**IDs are never derived from SailSys or TopYacht internal IDs.** Source system IDs may be
stored in alias/mapping tables for ingestion purposes only, never as primary keys.

---

## Things NOT to Do (Lessons from Old Prototype)

The old prototype is in the repo as domain knowledge only. **Do not replicate:**

1. **Static global registries** — no `static ArrayList` singletons on entity classes
2. **Mixed raw and derived fields** — raw records are immutable; derived state lives in
   separate layer objects
3. **Back-references from parent to child** — use index objects, not `boat.getEntries()`
4. **Club-specific logic in generic domain objects** — club concerns stay in importers/services
5. **Analysis logic on entity objects** — optimisation logic belongs in `HpfOptimiser` service
6. **save/restore pattern on entities** — symptom of mutable derived state on raw objects;
   working state lives in `BoatHpfEstimate` and friends, never on raw records

---

## Service Layer

Logic lives in services, not entities:

### Importers (populate Layer 1)
- `SailSysBoatImporter` — imports boats and IRC certificates from SailSys boat API/files
- `SailSysRaceImporter` — imports races, divisions, finishers from SailSys race API/files
- `TopYachtImporter` — scrapes TopYacht HTML result pages; handles multi-page merging
- `OrcImporter` — fetches ORC certificate feed XML; creates ORC certificates with variant flags
- `AmsImporter` — scrapes AMS certificate listings from raceyachts.org
- `BwpsImporter` — imports BWPS (CYCA) race results

### Analysis (Layers 2–3)
- `HandicapAnalyser` — mines paired observations to produce empirical conversion fits (ComparisonResult)
- `ConversionGraph` — directed graph of system×year×variant conversions with LinearFit edges
- `ReferenceNetworkBuilder` — DFS traversal of ConversionGraph to compute BoatReferenceFactors (steps 8–12)
- `AnalysisCache` — holds comparisons, reference factors, design factors, and navigation indexes

### Server
- `HpfServer` — Jetty 12.1 embedded server entry point (port 8080)
- `ImporterService` — manages importer lifecycle, scheduling, and admin configuration
- `AdminApiServlet` — REST API for boats/designs/races/importers/stats (paginated)
- `AnalysisServlet` — REST API for comparison analysis results
- `StaticResourceServlet` — serves admin frontend HTML/JS/CSS

### Store
- `DataStore` — central persistence; reads/writes JSON files; manages in-memory maps; fuzzy matching
- `AliasSeedLoader` — reads aliases.yaml for boat/design name aliases and sail number redirects
- `ClubSeedLoader` — reads clubs.yaml for club metadata and TopYacht URLs
- `DesignCatalogueLoader` — reads design.yaml for exclusions and design overrides
- `IdGenerator` — pure normalisation utilities for generating entity IDs
- `RemoveNonMeasurementCertificates` — housekeeping: strips PHS/CBH certificates from stored boats

---

## Persistence Layout

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

## Dataset Size

Small: ~600 boats each doing ~50 races/year. No need for premature optimisation. Prefer clarity.

---

## Build Status

Phase 1 (steps 1–12) is implemented:
1. **Data ingestion** — all six importers operational (SailSys boats/races, TopYacht, ORC, AMS, BWPS)
2. **Reference network** — HandicapAnalyser + ConversionGraph + ReferenceNetworkBuilder operational
3. **Admin webapp** — data browser, analysis charts (Plotly.js), import management

Phase 2 (steps 13–19) is not yet implemented:
4. HPF optimiser → optimised derived layer
5. Public-facing frontend → charts against real HPF output

---

## Licence

- Source code: Apache 2.0
- Derived data (HPF values, reference factors): CC BY-SA 4.0
- Raw race results: no claim — public record

See [LICENSE.md](LICENSE.md) for full details.
