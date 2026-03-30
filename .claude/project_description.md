# Project Description: Australian Yacht Racing Elapsed Time Database & Analysis

## Overview

The goal is to build an online database of elapsed times for all boats racing in Australia in recent years, with statistical analysis and graphical presentation tools. The primary motivating use case is Manly Yacht Club (MYC, NSW), which has a small fleet, meaning useful analysis requires drawing on data from many clubs across Australia.

---

## Data Collection

Six importers collect data from four source systems:

### SailSys
- `SailSysBoatImporter` — imports boat records and IRC certificates from the SailSys boat API. Supports local file mode (pre-downloaded JSON) and HTTP API mode with configurable throttling and caching.
- `SailSysRaceImporter` — imports races, divisions, and finishers from the SailSys race API. Scans from a configurable start ID; supports local file mode and HTTP API mode. Infers certificates from the `handicapCreatedFrom` field in race results, propagating the `nonSpinnaker` flag from race entries.

### TopYacht
- `TopYachtImporter` — scrapes TopYacht HTML result pages from a curated list of club URLs (maintained in `clubs.yaml`). Parses group captions to detect handicap system and variant flags (e.g. "Div 1 ORC NS WL results" → system=ORC, nonSpinnaker=true, windwardLeeward=true). Merges multi-page results (same race, different handicap systems) into a single race. Discovery of new TopYacht result pages is an ongoing activity.

### ORC Certificates
- `OrcImporter` — fetches the ORC Australian certificate XML feed from `data.orc.org`. For each certificate, fetches the detail page to extract GPH (converted to TCF via `600/GPH`). Detects non-spinnaker (NS), double-handed (DH), and club certificate variants from `CertType` and `FamilyName` fields.

### AMS Certificates
- `AmsImporter` — scrapes AMS certificate listings from raceyachts.org. Includes non-spinnaker and two-handed variants.

### BWPS (CYCA)
- `BwpsImporter` — imports BWPS race results from the Cruising Yacht Club of Australia. IRC-based races providing boat identity, elapsed times, and IRC handicaps used for scoring.

### Data Included
- Boat entry details (sail number, name, design, club)
- Elapsed times per race
- Measurement-based handicap certificates (IRC, ORC, AMS) with variant flags (nonSpinnaker, twoHanded, windwardLeeward, club)
- Handicap values used for scoring in measurement-system races (IRC, ORC, AMS AHC values)

### Data Excluded
- PHS handicap numbers assigned to boats (considered proprietary)
- Computed PHS race results (corrected times, positions derived from PHS)
- Skipper/owner names (not persisted)
- TopYacht `boid` internal identifiers (not persisted)
- Note: elapsed times from PHS races are valid and are used

---

## Boat Identity & Disambiguation

Boat identity is complex and must be handled carefully:
- Identified by sail number and name, neither of which is guaranteed unique.
- Name variations must be handled (e.g. "TenSixty" vs "1060", "Azzuro" vs "Komatsu Azzuro").
- Sail number typographic variations must be handled (e.g. "AUS-1234" vs "aus 1234").
- Disambiguation by club, design, and owner information where available.
- A boat may be associated with one or more clubs.

---

## Standard Candles

Statistical analysis is anchored on "standard candles" — boats for which performance can be independently estimated from measurement-based handicaps.

### Reference Factor Computation (Implemented)

Reference factors are computed by `ReferenceNetworkBuilder` via DFS traversal of a `ConversionGraph` — a directed graph of empirical linear fits between handicap system×year×variant nodes, built by `HandicapAnalyser` from paired observations across boats.

Certificate base weight is 1.0, with multiplicative discounts for certificate quality:

| Modifier | Weight | Applies when |
|---|---|---|
| Club certificate | configurable (default 0.9) | ORC club certs |
| Windward/leeward cert | 0.8× | ORC WL course-specific certs |
| Design-level fallback | 0.85× | No individual cert; using design aggregate |
| Race propagation | 0.7× | Derived indirectly from race co-participation |

Each certificate is converted to a target IRC-equivalent TCF for the current year via the shortest weighted path through the conversion graph. Factors degrade through each hop. Three separate factors are computed per boat: spinnaker, non-spinnaker, and two-handed.

### Candle Propagation
- `HandicapAnalyser` mines all boats with certificates in multiple systems or across years to produce paired observations
- `ConversionGraph` builds a directed graph from these pairs, with `LinearFit` edges (minimum R² = 0.75)
- `ReferenceNetworkBuilder` performs DFS from each boat's certificates through the graph to reach the target IRC node
- Design-level aggregation and race co-participation propagation extend coverage iteratively (up to 20 iterations)

---

## Handicap Estimation & Optimisation

### Back-Calculated Handicap
- For any race, the "back-calculated handicap" for a boat is the handicap it would have needed for all boats to share the same corrected time.
- This requires estimating the expected elapsed time for a hypothetical **1.000 handicap boat** in each race.

### Estimating the 1.000 Boat Reference Time
- Derived from the elapsed times of competing boats weighted by their candle quality and their estimated relationship to candles.
- Used to normalise race durations so that races of different lengths and conditions can be compared within a series.

### Self-Referential Circularity
- There is an acknowledged circularity: the 1.000 reference time is derived from boat elapsed times, which are then evaluated against that reference.
- Mitigation strategy: seed the optimisation with known candle values (deterministic anchor points), then use a **simulated annealing-style optimisation** to find the best-fit allocated handicaps for all boats consistent with the observed results.
- Preference for deterministic values wherever possible (e.g. a measurement certificate value is treated as a fixed anchor, not a variable).

### Asymmetry Principle
- It is easy for a boat to sail slower than its potential; it is essentially impossible to sail faster.
- This asymmetry should be incorporated into the statistical model: unexpectedly fast performances are treated with more suspicion than unexpectedly slow ones, except where weather/tide gates can explain them.
- In small fleets, an apparently outstanding result is more likely to reflect poor performance by others than a genuine exceptional performance.

---

## Race & Result Weighting

### Race-Level Weighting
- Some races will have extraordinary results making them unsuitable for analysis (e.g. severe weather splits, gear failure affecting many boats).
- Algorithms will be developed to detect and down-weight (rather than simply exclude) such races.
- Weighting at the race level is preferred over excluding individual boat results where possible.

### Boat-Level Weighting
- Individual boats may have poor performances that should be down-weighted.
- Since it is difficult to distinguish bad sailing from bad luck, caution will be applied before excluding individual boat results.
- Boats that are consistently variable (different crew, unforced errors) will naturally accumulate lower candle quality scores.

### Weather/Tide Gates
- Races where one group of boats experiences significantly different conditions will be flagged and potentially excluded or specially handled.

---

## Cross-Race Normalisation

To compare races within a series:
- The fleet composition changes race to race.
- Race durations vary.
- The 1.000 reference time is used to normalise all elapsed times to a common basis.
- This allows meaningful comparison of boat performance across races despite changing fleets and conditions.

---

## Website & Visualisation

An admin webapp is operational at `http://localhost:8080/` with three main sections:

### Data Browser (implemented)
- **Boats**: paginated list with search/filter, drill-down to certificates and race history
- **Designs**: paginated list with boat counts and design-level factors
- **Races**: paginated list with date/club filtering, drill-down to divisions and finishers
- **Manual merge**: admin UI for merging duplicate boats/designs, with alias YAML updates

### Analysis Charts (implemented)
- **Comparison scatter plots**: Plotly.js charts showing paired observations between handicap systems
- **Conversion graph visualisation**: dropdown selection of system×year×variant comparisons
- **Reference factor inspection**: per-boat factor details with generation tracking

### Import Management (implemented)
- **Importer controls**: run individual importers (SailSys, TopYacht, ORC, AMS, BWPS, analysis, reference-factors, build-indexes)
- **Scheduling**: configurable days and time for automatic import runs
- **Progress monitoring**: live status of running imports

### Future Views (not yet implemented — requires Phase 2)
- **Series view**: All races in a series plotted with back-calculated handicaps for each boat in each race
- **Boat comparison**: Select two or more specific boats and plot all races in which they have competed
- **Design comparison**: Select two or more designs and plot their relative performance

---

## Technology

- **Language:** Java 21
- **Server:** Jetty 12.1 embedded (plain Jakarta servlets — no Spring, no Spring Boot)
- **Data store:** Jackson JSON/YAML files (file-per-entity persistence under `hpf-data/`)
- **HTTP client:** Jetty HttpClient (for API calls and web scraping)
- **HTML parsing:** JSoup (TopYacht HTML scraping)
- **Fuzzy matching:** Apache Commons Text (Jaro-Winkler similarity for boat/design name matching)
- **Frontend:** Plain JavaScript, Plotly.js — no React, no build pipeline
- **Testing:** JUnit 5

---

## Scope

- Primary focus: Manly Yacht Club (MYC), NSW.
- Broader data collection: all available SailSys clubs in Australia, plus TopYacht clubs in NSW and/or Australia (via curated URL list).
- The project does not specifically target any single software vendor's clubs; mixing SailSys and TopYacht sources is deliberate.
